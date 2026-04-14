import { supabase, supabaseEnabled } from './supabase.js'
import { verifyEmails } from './verify.js'

const CLAIM_LIMIT = 50
const POST_BATCH_FLUSH_INTERVAL_MS = 500
const PROCESS_CATCH_ALL_TTL_MS = 24 * 60 * 60 * 1000 // 24h

/**
 * @typedef {object} BatchMetrics
 * @property {number} chunksProcessed
 * @property {number} emailsVerifiedViaVps - Uncached emails that hit the SMTP layer
 * @property {number} emailsServedFromDbCache - Results that came from email_verifications cache
 * @property {number} catchAllProbesRun - Number of actual catch-all SMTP probes executed
 * @property {number} catchAllProbesSavedByBatchCache - Saved by per-batch domain memo
 * @property {number} catchAllProbesSavedByProcessCache - Saved by process-level domain memo
 * @property {number} chunkMsTotal - Wall time inside processChunk across the batch
 * @property {number} chunkMsMax - Longest single-chunk wall time
 * @property {number} smtpMsTotal - Wall time inside verifyEmails across the batch
 * @property {Record<string, number>} statusCounts - valid / invalid / catch_all / unknown / risky
 */

/**
 * Registry of currently-running batch jobs. Keyed by batch ID. Used so that
 * the same batch can't be started twice concurrently, and so `/status` can
 * report in-process state even when DB hasn't heartbeated yet.
 *
 * @type {Map<string, { status: 'running' | 'cancelled', startedAt: number, metrics: BatchMetrics }>}
 */
const activeBatches = new Map()

/**
 * Process-level catch-all cache with TTL. Shared across all batches so that
 * repeat validations of the same domain (common when re-running small batches
 * targeting the same companies) skip the SMTP probe.
 *
 * The value `null` is a confirmed "not catch-all" — we cache negatives too.
 *
 * @type {Map<string, { isCatchAll: boolean, expiresAt: number }>}
 */
const processCatchAllCache = new Map()

/** Build a fresh BatchMetrics object. @returns {BatchMetrics} */
function newMetrics() {
  return {
    chunksProcessed: 0,
    emailsVerifiedViaVps: 0,
    emailsServedFromDbCache: 0,
    catchAllProbesRun: 0,
    catchAllProbesSavedByBatchCache: 0,
    catchAllProbesSavedByProcessCache: 0,
    chunkMsTotal: 0,
    chunkMsMax: 0,
    smtpMsTotal: 0,
    statusCounts: { valid: 0, invalid: 0, catch_all: 0, unknown: 0, risky: 0 },
  }
}

/**
 * Layer a per-batch Map on top of the process-level cache. Reads fall
 * through to the process cache (respecting TTL), and writes propagate
 * upward so future batches benefit.
 *
 * Returned object follows the Map surface used by verifyEmails
 * (`get`/`set`/`has`) so the existing plumbing is unchanged.
 *
 * @param {BatchMetrics} metrics
 */
function buildBatchCatchAllCache(metrics) {
  /** @type {Map<string, boolean>} */
  const local = new Map()

  return {
    /** @param {string} domain */
    has(domain) {
      if (local.has(domain)) {
        metrics.catchAllProbesSavedByBatchCache++
        return true
      }
      const entry = processCatchAllCache.get(domain)
      if (entry && entry.expiresAt > Date.now()) {
        // Promote into local so has/get are consistent within the batch.
        local.set(domain, entry.isCatchAll)
        metrics.catchAllProbesSavedByProcessCache++
        return true
      }
      if (entry) processCatchAllCache.delete(domain)
      return false
    },
    /** @param {string} domain */
    get(domain) {
      return local.get(domain)
    },
    /**
     * Called by verifySingle after a fresh SMTP probe. Records the result
     * in both the per-batch cache and the process-level cache (with TTL).
     * @param {string} domain
     * @param {boolean} isCatchAll
     */
    set(domain, isCatchAll) {
      local.set(domain, isCatchAll)
      processCatchAllCache.set(domain, {
        isCatchAll,
        expiresAt: Date.now() + PROCESS_CATCH_ALL_TTL_MS,
      })
      metrics.catchAllProbesRun++
    },
    _localSize: () => local.size,
  }
}

/** @param {string|null|undefined} email */
function normalizeEmail(email) {
  if (!email || typeof email !== 'string') return ''
  return email.trim().toLowerCase()
}

/**
 * Map verification result status (hyphenated) to sourced_contacts status (underscored).
 * @param {string} status
 */
function toContactStatus(status) {
  return (status || '').replace(/-/g, '_')
}

/**
 * Claim a chunk of pending items for this batch via the Supabase RPC.
 * Uses FOR UPDATE SKIP LOCKED semantics so multiple workers don't collide.
 *
 * @param {string} batchId
 * @param {number} limit
 * @returns {Promise<Array<{ id: string, email: string, contact_ids: string[] }>>}
 */
async function claimItems(batchId, limit) {
  const { data, error } = await supabase.rpc('claim_validation_batch_items', {
    p_batch_id: batchId,
    p_limit: limit,
  })
  if (error) throw error
  return data || []
}

/**
 * Load cached email_verifications rows for a list of emails (30 day freshness).
 * @param {string[]} emails
 */
async function loadCached(emails) {
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString()
  const { data, error } = await supabase
    .from('email_verifications')
    .select('*')
    .in('email', emails)
    .gte('verified_at', thirtyDaysAgo)
  if (error) throw error
  return data || []
}

/**
 * Upsert verification results into the long-term cache.
 * @param {Array<import('./verify.js').VerificationResult>} results
 */
async function cacheResults(results) {
  if (results.length === 0) return
  const rows = []
  for (const r of results) {
    const email = normalizeEmail(r.email)
    const at = email.indexOf('@')
    if (at <= 0 || at >= email.length - 1) continue
    const domain = email.slice(at + 1)
    if (!domain) continue
    rows.push({
      email,
      domain,
      status: r.status,
      smtp_response_code: r.smtp_response_code,
      mx_host: r.mx_host,
      is_catch_all: r.is_catch_all,
      is_disposable: r.is_disposable,
      verified_at: new Date().toISOString(),
    })
  }
  if (rows.length === 0) return
  const { error } = await supabase.from('email_verifications').upsert(rows, { onConflict: 'email' })
  if (error) console.error('[batch-runner] email_verifications upsert failed:', error.message)
}

/**
 * Commit the result for a single validation_batch_item:
 *   - update all linked sourced_contacts
 *   - mark the item completed (or failed)
 *
 * @param {string} itemId
 * @param {string[]} contactIds
 * @param {string} verificationStatus
 * @param {string} [errorMessage]
 * @param {import('fastify').FastifyBaseLogger | typeof console} [logger]
 */
async function commitItemResult(itemId, contactIds, verificationStatus, errorMessage, logger = console) {
  if (contactIds?.length) {
    const { error } = await supabase
      .from('sourced_contacts')
      .update({ verification_status: verificationStatus })
      .in('id', contactIds)
    if (error) {
      logger.error({ itemId, err: error.message }, '[batch-runner] sourced_contacts update failed')
      const { error: itemMarkError } = await supabase
        .from('validation_batch_items')
        .update({
          status: 'failed',
          verification_status: verificationStatus,
          error_message: `Contact update failed: ${error.message}`,
          updated_at: new Date().toISOString(),
        })
        .eq('id', itemId)
      if (itemMarkError) {
        logger.error({ itemId, err: itemMarkError.message }, '[batch-runner] validation_batch_items mark failed after contact error')
        throw itemMarkError
      }
      return { ok: false }
    }
  }

  const { error: itemUpdateError } = await supabase
    .from('validation_batch_items')
    .update({
      status: errorMessage ? 'failed' : 'completed',
      verification_status: verificationStatus,
      error_message: errorMessage ?? null,
      updated_at: new Date().toISOString(),
    })
    .eq('id', itemId)
  if (itemUpdateError) {
    logger.error({ itemId, err: itemUpdateError.message }, '[batch-runner] validation_batch_items update failed')
    throw itemUpdateError
  }

  return { ok: !errorMessage }
}

/**
 * Bump the batch progress counters.
 *
 * @param {string} batchId
 * @param {{ processed: number, cached: number, verified: number, failed: number }} deltas
 */
async function bumpBatchCounters(batchId, deltas, logger) {
  const { error } = await supabase.rpc('increment_validation_batch_progress', {
    p_batch_id: batchId,
    p_processed: deltas.processed,
    p_cached: deltas.cached,
    p_verified: deltas.verified,
    p_failed: deltas.failed,
  })
  if (error) {
    logger.error({ batchId, err: error.message }, '[batch-runner] increment_validation_batch_progress failed')
    throw error
  }
}

/**
 * Promote websites to 'verified' when at least one of their contacts ended up valid.
 * @param {string[]} contactIds
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function syncVerifiedWebsiteStatuses(contactIds, logger) {
  if (!contactIds?.length) return
  const CHUNK = 200

  /** @type {Array<{ website_id: string }>} */
  const verifiedContacts = []
  for (let i = 0; i < contactIds.length; i += CHUNK) {
    const chunk = contactIds.slice(i, i + CHUNK)
    const { data, error } = await supabase
      .from('sourced_contacts')
      .select('website_id')
      .in('id', chunk)
      .eq('verification_status', 'valid')
    if (error) {
      logger.error({ err: error.message, chunkSize: chunk.length }, '[batch-runner] syncVerifiedWebsiteStatuses select failed')
      throw error
    }
    if (data) verifiedContacts.push(...data)
  }

  if (verifiedContacts.length === 0) return
  const websiteIds = [...new Set(verifiedContacts.map((c) => c.website_id))]
  for (let i = 0; i < websiteIds.length; i += CHUNK) {
    const chunk = websiteIds.slice(i, i + CHUNK)
    const { error } = await supabase
      .from('sourced_websites')
      .update({ status: 'verified', updated_at: new Date().toISOString() })
      .in('id', chunk)
      .in('status', ['enriched', 'campaign_ready'])
    if (error) {
      logger.error({ err: error.message, chunkSize: chunk.length }, '[batch-runner] sourced_websites verify update failed')
      throw error
    }
  }
}

/**
 * Finalize the batch — mark completed/failed and sync website statuses.
 * @param {string} batchId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function finalize(batchId, logger) {
  const { data: batch, error } = await supabase
    .from('validation_batches')
    .select('contact_ids, failed, status')
    .eq('id', batchId)
    .maybeSingle()
  if (error) {
    logger.error({ batchId, err: error.message }, '[batch-runner] finalize: load batch row failed')
    throw error
  }

  if (!batch) return
  // If the batch was cancelled (admin marks it `failed`) don't overwrite it.
  if (batch.status === 'failed' || batch.status === 'completed') return

  if (batch.contact_ids?.length) {
    await syncVerifiedWebsiteStatuses(batch.contact_ids, logger)
  }

  const finalStatus = (batch.failed ?? 0) > 0 ? 'failed' : 'completed'
  const { error: finalUpdateError } = await supabase
    .from('validation_batches')
    .update({ status: finalStatus, updated_at: new Date().toISOString() })
    .eq('id', batchId)
  if (finalUpdateError) {
    logger.error({ batchId, err: finalUpdateError.message }, '[batch-runner] finalize: batch status update failed')
    throw finalUpdateError
  }
}

/**
 * Poll the batch row for cancellation. Cheap read, one row.
 * The admin cancel endpoint sets status to 'failed' (the DB check constraint
 * doesn't currently include 'cancelled'), so that's our cancellation signal.
 * @param {string} batchId
 */
async function isCancelled(batchId, logger) {
  const { data, error } = await supabase
    .from('validation_batches')
    .select('status')
    .eq('id', batchId)
    .maybeSingle()
  if (error) {
    logger.error({ batchId, err: error.message }, '[batch-runner] isCancelled: status poll failed')
    throw error
  }
  return data?.status === 'failed' || data?.status === 'cancelled'
}

/**
 * Process the items in `chunk` by running verifyEmails, then commit each
 * result to Supabase. Catch-all results for repeated domains inside `chunk`
 * are short-circuited by the per-batch + process-level catch-all cache so
 * the actual SMTP probe runs at most once per domain per 24h.
 *
 * Tracks per-chunk timing into `metrics` for the final batch summary.
 *
 * Per-email progress model: the batch counter (`validation_batches.processed`)
 * and per-item status both commit *as each email resolves*, not at chunk
 * boundaries. This is what drives live UI progress — without it, the progress
 * bar would jump 50 at a time every ~75 seconds.
 *
 * @param {string} batchId
 * @param {Array<{ id: string, email: string, contact_ids: string[] }>} chunk
 * @param {Map<string, import('./verify.js').VerificationResult>} cacheMap
 * @param {ReturnType<typeof buildBatchCatchAllCache>} catchAllDomainCache
 * @param {BatchMetrics} metrics
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function processChunk(batchId, chunk, cacheMap, catchAllDomainCache, metrics, logger) {
  const chunkStart = Date.now()

  /** @type {Array<{ id: string, email: string, contact_ids: string[] }>} */
  const fromCache = []
  /** @type {Array<{ id: string, email: string, contact_ids: string[] }>} */
  const fromVps = []

  for (const item of chunk) {
    if (cacheMap.has(normalizeEmail(item.email))) fromCache.push(item)
    else fromVps.push(item)
  }

  const results = { processed: 0, cached: 0, verified: 0, failed: 0 }

  /**
   * Commit one item + bump the batch counter by 1. Called from both the
   * cache fast path and the per-email SMTP callback so the progress bar
   * advances one row at a time regardless of which path served the result.
   * @param {{ id: string, contact_ids: string[] }} item
   * @param {string} vs - Verification status (valid/invalid/catch_all/unknown/risky)
   * @param {'cache' | 'vps'} source
   */
  async function commitOne(item, vs, source) {
    const { ok } = await commitItemResult(item.id, item.contact_ids, vs, undefined, logger)
    await bumpBatchCounters(
      batchId,
      {
        processed: 1,
        cached: source === 'cache' && ok ? 1 : 0,
        verified: source === 'vps' && ok ? 1 : 0,
        failed: ok ? 0 : 1,
      },
      logger,
    )
    results.processed++
    if (source === 'cache') {
      metrics.emailsServedFromDbCache++
      if (ok) results.cached++
      else results.failed++
    } else {
      metrics.emailsVerifiedViaVps++
      if (ok) results.verified++
      else results.failed++
    }
    metrics.statusCounts[vs] = (metrics.statusCounts[vs] ?? 0) + 1
  }

  // Fast path: DB-cache hits commit per-email immediately.
  for (const item of fromCache) {
    const hit = cacheMap.get(normalizeEmail(item.email))
    const vs = toContactStatus(hit.status)
    await commitOne(item, vs, 'cache')
  }

  if (fromVps.length > 0) {
    const smtpStart = Date.now()
    /** All batch rows per normalized address (same email can appear on multiple items). */
    /** @type {Map<string, Array<{ id: string, email: string, contact_ids: string[] }>>} */
    const itemsByEmail = new Map()
    for (const i of fromVps) {
      const key = normalizeEmail(i.email)
      let list = itemsByEmail.get(key)
      if (!list) {
        list = []
        itemsByEmail.set(key, list)
      }
      list.push(i)
    }

    // One SMTP probe per unique address; onResult fans out to every item sharing that email.
    const uniqueEmails = []
    const seenKeys = new Set()
    for (const i of fromVps) {
      const key = normalizeEmail(i.email)
      if (seenKeys.has(key)) continue
      seenKeys.add(key)
      uniqueEmails.push(i.email)
    }

    /** Cache-upsert batch: accumulate fresh results and flush once per chunk
     *  to avoid hammering email_verifications with N single-row upserts. */
    /** @type {import('./verify.js').VerificationResult[]} */
    const fresh = []

    const seenItemIds = new Set()
    await verifyEmails(uniqueEmails, {
      logger,
      catchAllDomainCache,
      onResult: async (r) => {
        const key = normalizeEmail(r.email)
        const items = itemsByEmail.get(key)
        if (!items?.length) return
        const vs = toContactStatus(r.status)
        for (const item of items) {
          try {
            await commitOne(item, vs, 'vps')
            seenItemIds.add(item.id)
          } catch (err) {
            logger.error(
              { itemId: item.id, email: key, err: err instanceof Error ? err.message : String(err) },
              '[batch-runner] commitOne failed for shared-address item',
            )
          }
        }
        fresh.push(r)
      },
    })

    // Items never marked seen: no verify result, or verify succeeded but `onResult` failed
    // before commit (e.g. DB error) — still need DB + counters so the batch can finalize.
    for (const item of fromVps) {
      if (seenItemIds.has(item.id)) continue
      await commitItemResult(item.id, item.contact_ids, 'unknown', 'No result returned', logger)
      await bumpBatchCounters(batchId, { processed: 1, cached: 0, verified: 0, failed: 1 }, logger)
      results.processed++
      results.failed++
      metrics.statusCounts.unknown = (metrics.statusCounts.unknown ?? 0) + 1
    }

    metrics.smtpMsTotal += Date.now() - smtpStart
    await cacheResults(fresh)
  }

  const chunkMs = Date.now() - chunkStart
  metrics.chunksProcessed++
  metrics.chunkMsTotal += chunkMs
  if (chunkMs > metrics.chunkMsMax) metrics.chunkMsMax = chunkMs

  return results
}

/**
 * Run a validation batch from start to finish. Idempotent — if the batch is
 * already running in-process, returns the existing handle. Safe to call
 * multiple times.
 *
 * @param {string} batchId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
export async function runBatch(batchId, logger) {
  if (!supabaseEnabled) {
    throw new Error('Batch runner requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY')
  }

  if (activeBatches.has(batchId)) {
    logger.info({ batchId }, '[batch-runner] Batch already running in this process — skipping kickoff')
    return
  }

  const metrics = newMetrics()
  const handle = {
    status: /** @type {'running' | 'cancelled'} */ ('running'),
    startedAt: Date.now(),
    metrics,
  }
  activeBatches.set(batchId, handle)
  logger.info({ batchId }, '[batch-runner] Starting batch')

  // Move interrupted → running if necessary.
  const { error: resumeRunningError } = await supabase
    .from('validation_batches')
    .update({ status: 'running', updated_at: new Date().toISOString() })
    .eq('id', batchId)
    .in('status', ['interrupted'])
  if (resumeRunningError) {
    activeBatches.delete(batchId)
    logger.error(
      { batchId, err: resumeRunningError.message },
      '[batch-runner] Failed to set batch status to running (interrupted→running)',
    )
    throw resumeRunningError
  }

  const catchAllDomainCache = buildBatchCatchAllCache(metrics)

  try {
    while (true) {
      if (handle.status === 'cancelled' || (await isCancelled(batchId, logger))) {
        logger.warn({ batchId }, '[batch-runner] Batch cancelled — exiting loop')
        break
      }

      const chunk = await claimItems(batchId, CLAIM_LIMIT)
      if (chunk.length === 0) break

      // Cache lookup for everything we're about to process.
      const emails = chunk.map((i) => normalizeEmail(i.email))
      const cached = await loadCached(emails)
      const cacheMap = new Map(cached.map((r) => [normalizeEmail(r.email), r]))

      // processChunk now commits per-email AND bumps the batch counter per-email,
      // so the progress bar advances one row at a time. No chunk-level bumpBatchCounters
      // needed — that would double-count.
      const deltas = await processChunk(batchId, chunk, cacheMap, catchAllDomainCache, metrics, logger)

      logger.info(
        {
          batchId,
          chunk: chunk.length,
          dbCacheHits: cacheMap.size,
          ...deltas,
          elapsedMs: Date.now() - handle.startedAt,
        },
        '[batch-runner] Chunk processed',
      )

      // Tiny yield so a sudden cancel can be detected quickly.
      await new Promise((r) => setTimeout(r, POST_BATCH_FLUSH_INTERVAL_MS))
    }

    await finalize(batchId, logger)

    const totalEmailsProcessed = metrics.emailsVerifiedViaVps + metrics.emailsServedFromDbCache
    const elapsedMs = Date.now() - handle.startedAt
    const emailsPerSec = elapsedMs > 0 ? (totalEmailsProcessed * 1000) / elapsedMs : 0
    const totalCatchAllOpportunities =
      metrics.catchAllProbesRun +
      metrics.catchAllProbesSavedByBatchCache +
      metrics.catchAllProbesSavedByProcessCache
    const catchAllSaveRate =
      totalCatchAllOpportunities > 0
        ? (metrics.catchAllProbesSavedByBatchCache + metrics.catchAllProbesSavedByProcessCache) /
          totalCatchAllOpportunities
        : 0
    const dbCacheHitRate =
      totalEmailsProcessed > 0 ? metrics.emailsServedFromDbCache / totalEmailsProcessed : 0

    logger.info(
      {
        batchId,
        elapsedMs,
        elapsedSec: Math.round(elapsedMs / 1000),
        totalEmailsProcessed,
        emailsPerSec: Number(emailsPerSec.toFixed(2)),
        chunksProcessed: metrics.chunksProcessed,
        chunkMsAvg:
          metrics.chunksProcessed > 0
            ? Math.round(metrics.chunkMsTotal / metrics.chunksProcessed)
            : 0,
        chunkMsMax: metrics.chunkMsMax,
        smtpMsTotal: metrics.smtpMsTotal,
        dbCache: {
          hits: metrics.emailsServedFromDbCache,
          hitRate: Number((dbCacheHitRate * 100).toFixed(1)),
        },
        catchAll: {
          probesRun: metrics.catchAllProbesRun,
          savedByBatchCache: metrics.catchAllProbesSavedByBatchCache,
          savedByProcessCache: metrics.catchAllProbesSavedByProcessCache,
          saveRatePct: Number((catchAllSaveRate * 100).toFixed(1)),
        },
        statusCounts: metrics.statusCounts,
      },
      '[batch-runner] Batch summary',
    )
  } catch (err) {
    logger.error({ batchId, err: err instanceof Error ? err.message : String(err) }, '[batch-runner] Batch failed')
    const { error: markFailedError } = await supabase
      .from('validation_batches')
      .update({
        status: 'failed',
        updated_at: new Date().toISOString(),
      })
      .eq('id', batchId)
    if (markFailedError) {
      logger.error({ batchId, err: markFailedError.message }, '[batch-runner] Failed to persist batch failed status')
    }
  } finally {
    activeBatches.delete(batchId)
  }
}

/**
 * Mark an in-process batch for cancellation. Also sets the DB row so other
 * listeners see it.
 * @param {string} batchId
 */
export async function cancelBatch(batchId, logger = console) {
  const handle = activeBatches.get(batchId)
  if (handle) handle.status = 'cancelled'

  if (!supabaseEnabled) return
  // `failed` is the terminal cancel status allowed by the DB check constraint.
  const { error } = await supabase
    .from('validation_batches')
    .update({ status: 'failed', updated_at: new Date().toISOString() })
    .eq('id', batchId)
  if (error) {
    logger.error({ batchId, err: error.message }, '[batch-runner] cancelBatch: status update failed')
    throw error
  }
}

/**
 * Read-only status for a batch. Returns whatever Supabase has plus whether
 * we're actively processing in-memory.
 * @param {string} batchId
 */
export async function getBatchStatus(batchId) {
  if (!supabaseEnabled) return { error: 'Supabase not configured' }

  const { data: batch, error } = await supabase
    .from('validation_batches')
    .select('*')
    .eq('id', batchId)
    .maybeSingle()
  if (error) return { error: error.message }
  if (!batch) return { error: 'Batch not found' }

  const handle = activeBatches.get(batchId)
  return {
    batch_id: batch.id,
    status: batch.status,
    total: batch.total,
    processed: batch.processed,
    cached: batch.cached,
    verified: batch.verified,
    failed: batch.failed,
    remaining: batch.total - batch.processed,
    in_process: !!handle,
    in_process_started_at: handle?.startedAt ?? null,
    updated_at: batch.updated_at,
  }
}

/**
 * Read-only metrics for an in-flight or recently-completed batch.
 * Only returns live metrics while the batch is still running in this process;
 * after it completes, the handle is dropped and you'll get `in_process: false`.
 *
 * @param {string} batchId
 */
export function getBatchMetrics(batchId) {
  const handle = activeBatches.get(batchId)
  if (!handle) return { in_process: false }

  const elapsedMs = Date.now() - handle.startedAt
  const totalEmailsProcessed =
    handle.metrics.emailsVerifiedViaVps + handle.metrics.emailsServedFromDbCache
  const emailsPerSec = elapsedMs > 0 ? (totalEmailsProcessed * 1000) / elapsedMs : 0

  return {
    in_process: true,
    started_at: handle.startedAt,
    elapsed_ms: elapsedMs,
    emails_processed: totalEmailsProcessed,
    emails_per_sec: Number(emailsPerSec.toFixed(2)),
    ...handle.metrics,
  }
}

/**
 * Snapshot of the shared process-level catch-all cache. Intended for ops
 * inspection — not called by the batch pipeline.
 */
export function getProcessCacheSnapshot() {
  const now = Date.now()
  let fresh = 0
  let expired = 0
  for (const [, entry] of processCatchAllCache) {
    if (entry.expiresAt > now) fresh++
    else expired++
  }
  return {
    total_entries: processCatchAllCache.size,
    fresh,
    expired,
    ttl_ms: PROCESS_CATCH_ALL_TTL_MS,
  }
}

/**
 * On service startup, reclaim any stale processing items (process crashed
 * mid-batch) and auto-resume any batches still in 'running' state.
 *
 * @param {import('fastify').FastifyBaseLogger} logger
 */
export async function recoverOnStartup(logger) {
  if (!supabaseEnabled) {
    logger.warn('[batch-runner] Supabase not configured — skipping startup recovery')
    return
  }

  try {
    const { data: reclaimed, error: reclaimError } = await supabase.rpc('reclaim_stale_validation_items', {
      p_stale_seconds: 30,
    })
    if (reclaimError) {
      logger.error({ err: reclaimError.message }, '[batch-runner] reclaim_stale_validation_items failed')
      throw reclaimError
    }
    if (reclaimed && reclaimed > 0) {
      logger.info({ reclaimed }, '[batch-runner] Reclaimed stale items on startup')
    }

    const { data: resumable, error: listError } = await supabase
      .from('validation_batches')
      .select('id')
      .in('status', ['running', 'interrupted'])
    if (listError) {
      logger.error({ err: listError.message }, '[batch-runner] list resumable validation_batches failed')
      throw listError
    }

    for (const batch of resumable || []) {
      logger.info({ batchId: batch.id }, '[batch-runner] Auto-resuming batch on startup')
      runBatch(batch.id, logger).catch((err) => {
        logger.error(
          { batchId: batch.id, err: err instanceof Error ? err.message : String(err) },
          '[batch-runner] Auto-resume failed',
        )
      })
    }
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : String(err) }, '[batch-runner] Startup recovery failed')
  }
}
