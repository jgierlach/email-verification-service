import { supabase, supabaseEnabled } from './supabase.js'
import { prospeoFallbackEnrich, PROSPEO_COST_PER_CREDIT_USD } from './prospeo.js'

const CLAIM_LIMIT = 25
const HUNTER_DELAY_MS = 1000
const WEBSITE_VALIDATION_TIMEOUT_MS = 8000

/**
 * @typedef {object} EnrichmentMetrics
 * @property {number} itemsProcessed
 * @property {number} itemsWithContacts
 * @property {number} totalContactsFound
 * @property {number} hunterHits - items where Hunter returned >= 1 email
 * @property {number} hunterMisses - items where Hunter returned 0 emails
 * @property {number} prospeoRuns - times the fallback was invoked
 * @property {number} prospeoRescues - prospeo runs that yielded >= 1 contact
 * @property {number} prospeoCredits
 * @property {number} sitesDisqualified - failed website validation (unreachable / redirects)
 * @property {number} hunterCreditsUsed
 * @property {Record<string, number>} finalStatusCounts - e.g. enriched / enrichment_failed / disqualified
 */

/**
 * Registry of batches currently running in this process.
 * @type {Map<string, { status: 'running' | 'cancelled', startedAt: number, metrics: EnrichmentMetrics }>}
 */
const activeBatches = new Map()

const DECISION_MAKER_TITLES = [
  'owner', 'founder', 'ceo', 'executive director', 'pastor', 'senior pastor',
  'president', 'managing partner', 'administrator', 'director', 'principal',
  'manager', 'partner',
]

/** @param {string|null} title */
function isDecisionMaker(title) {
  if (!title) return false
  const lower = title.toLowerCase()
  return DECISION_MAKER_TITLES.some((t) => lower.includes(t))
}

/** @returns {EnrichmentMetrics} */
function newMetrics() {
  return {
    itemsProcessed: 0,
    itemsWithContacts: 0,
    totalContactsFound: 0,
    hunterHits: 0,
    hunterMisses: 0,
    prospeoRuns: 0,
    prospeoRescues: 0,
    prospeoCredits: 0,
    sitesDisqualified: 0,
    hunterCreditsUsed: 0,
    finalStatusCounts: {},
  }
}

/**
 * Validate a website is reachable and not a parked/redirected domain.
 * Uses a HEAD request with a short timeout. Falls back to GET if HEAD is rejected.
 * @param {string} domain
 * @returns {Promise<{ valid: boolean, reason?: string }>}
 */
async function validateWebsite(domain) {
  const url = `https://${domain}`

  /**
   * @param {string} method
   * @returns {Promise<Response>}
   */
  async function fetchWithTimeout(method) {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), WEBSITE_VALIDATION_TIMEOUT_MS)
    try {
      return await fetch(url, {
        method,
        signal: controller.signal,
        redirect: 'follow',
      })
    } finally {
      clearTimeout(timeout)
    }
  }

  try {
    let res = await fetchWithTimeout('HEAD')
    if (res.status === 405) {
      res = await fetchWithTimeout('GET')
    }

    const finalUrl = res.url
    if (finalUrl) {
      try {
        const finalDomain = new URL(finalUrl).hostname.replace(/^www\./, '')
        const originalDomain = domain.replace(/^www\./, '')
        if (finalDomain !== originalDomain && !finalDomain.endsWith(`.${originalDomain}`)) {
          return { valid: false, reason: `redirects to ${finalDomain}` }
        }
      } catch {
        // URL parsing failed — not a blocker
      }
    }

    if (res.ok || res.status === 403) {
      return { valid: true }
    }
    return { valid: false, reason: `HTTP ${res.status}` }
  } catch (err) {
    if (err.name === 'AbortError') {
      return { valid: false, reason: 'timeout' }
    }
    return { valid: false, reason: err.message }
  }
}

/**
 * Parse a Hunter API error response into a useful message.
 * @param {any} data
 * @param {number} status
 */
function hunterApiErrorMessage(data, status) {
  if (data && typeof data === 'object' && Array.isArray(data.errors) && data.errors.length > 0) {
    const first = data.errors[0]
    if (first && typeof first === 'object') {
      if (typeof first.details === 'string') return first.details
      if (typeof first.id === 'string') return first.id
    }
  }
  return `Hunter API error ${status}`
}

/**
 * Claim a chunk of pending items for this batch via Supabase RPC.
 * @param {string} batchId
 * @param {number} limit
 * @returns {Promise<Array<{ id: string, website_id: string }>>}
 */
async function claimItems(batchId, limit) {
  const { data, error } = await supabase.rpc('claim_enrichment_batch_items', {
    p_batch_id: batchId,
    p_limit: limit,
  })
  if (error) throw error
  return data || []
}

/**
 * Poll cancellation signal. Admin cancel endpoint flips status to `failed` (the
 * DB check constraint doesn't include 'cancelled').
 * @param {string} batchId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function isCancelled(batchId, logger) {
  const { data, error } = await supabase
    .from('enrichment_batches')
    .select('status')
    .eq('id', batchId)
    .maybeSingle()
  if (error) {
    logger.error({ batchId, err: error.message }, '[enrichment-runner] isCancelled poll failed')
    throw error
  }
  return data?.status === 'failed' || data?.status === 'cancelled'
}

/**
 * Atomically bump batch counters. Doubles as heartbeat (updated_at).
 * @param {string} batchId
 * @param {{ processed?: number, contacts_found?: number, sites_with_contacts?: number, status?: string }} updates
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function bumpBatchCounters(batchId, updates, logger) {
  const { error } = await supabase.rpc('increment_enrichment_batch_progress', {
    p_batch_id: batchId,
    p_processed: updates.processed ?? 0,
    p_contacts_found: updates.contacts_found ?? 0,
    p_sites: updates.sites_with_contacts ?? 0,
    p_status: updates.status ?? null,
  })
  if (error) {
    logger.error({ batchId, err: error.message }, '[enrichment-runner] bumpBatchCounters failed')
  }
}

/**
 * Mark a batch item finished and bump per-item batch counters. Drives the
 * per-row live progress: the enrich UI's Realtime subscription will see both
 * the item-row status flip AND the batch counter bump.
 *
 * @param {string} batchId
 * @param {string} itemId
 * @param {string} status - completed | failed | skipped
 * @param {number} contactsFound
 * @param {string} [errorMessage]
 * @param {import('fastify').FastifyBaseLogger} [logger]
 */
async function markItemDone(batchId, itemId, status, contactsFound, errorMessage, logger = console) {
  const itemUpdate = { status, contacts_found: contactsFound, updated_at: new Date().toISOString() }
  if (errorMessage) itemUpdate.error_message = errorMessage

  const { error } = await supabase
    .from('enrichment_batch_items')
    .update(itemUpdate)
    .eq('id', itemId)
  if (error) {
    logger.error({ itemId, err: error.message }, '[enrichment-runner] enrichment_batch_items update failed')
  }

  await bumpBatchCounters(
    batchId,
    {
      processed: 1,
      contacts_found: contactsFound,
      sites_with_contacts: contactsFound > 0 ? 1 : 0,
    },
    logger,
  )
}

/**
 * Insert verified contacts for a website. Skips duplicates by (website_id, email).
 * Returns number inserted and any insert errors (for diagnostic logging).
 *
 * @param {string} websiteId
 * @param {Array<object>} contactRows - Must have the exact sourced_contacts column shape
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function insertContacts(websiteId, contactRows, logger) {
  let inserted = 0
  /** @type {Array<{ email: string | null, message: string, code?: string }>} */
  const insertErrors = []

  for (const contact of contactRows) {
    if (contact.email) {
      const { data: existing } = await supabase
        .from('sourced_contacts')
        .select('id')
        .eq('website_id', websiteId)
        .eq('email', contact.email)
        .maybeSingle()

      if (existing) continue
    }

    const { error } = await supabase
      .from('sourced_contacts')
      .insert([{ ...contact, website_id: websiteId }])

    if (!error) {
      inserted++
    } else if (insertErrors.length < 12) {
      insertErrors.push({
        email: contact.email ?? null,
        message: error.message,
        code: error.code,
      })
    }
  }

  if (insertErrors.length > 0) {
    logger.warn({ websiteId, insertErrors }, '[enrichment-runner] some contact inserts failed')
  }
  return { inserted, insertErrors }
}

/**
 * Map a Hunter email entry into the sourced_contacts row shape.
 * @param {any} email
 */
function hunterEmailToContactRow(email) {
  return {
    first_name: email.first_name || null,
    last_name: email.last_name || null,
    name: [email.first_name, email.last_name].filter(Boolean).join(' ') || null,
    title: email.position || null,
    email: email.value || null,
    phone: email.phone_number || null,
    source: 'hunter',
    source_metadata: email,
    is_decision_maker: isDecisionMaker(email.position),
  }
}

/**
 * Run Prospeo fallback for a single website. Logs to enrichment_log and
 * persists returned contacts. Returns number of rows actually inserted.
 *
 * @param {string} websiteId
 * @param {string} domain
 * @param {EnrichmentMetrics} metrics
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function runProspeoFallback(websiteId, domain, metrics, logger) {
  metrics.prospeoRuns++

  let result
  try {
    result = await prospeoFallbackEnrich(domain, logger)
  } catch (err) {
    if (err.fatal) throw err
    logger.error({ domain, err: err.message }, '[enrichment-runner] prospeo fallback errored')
    await supabase.from('enrichment_log').insert([{
      website_id: websiteId,
      step: 'prospeo',
      success: false,
      contacts_found: 0,
      credits_used: 0,
      cost_usd: 0,
      response_metadata: { error: err.message },
    }])
    return 0
  }

  const { inserted, insertErrors } = await insertContacts(websiteId, result.contacts, logger)
  metrics.prospeoCredits += result.creditsUsed
  if (inserted > 0) metrics.prospeoRescues++

  await supabase.from('enrichment_log').insert([{
    website_id: websiteId,
    step: 'prospeo',
    success: inserted > 0,
    contacts_found: inserted,
    credits_used: result.creditsUsed,
    cost_usd: Number((result.creditsUsed * PROSPEO_COST_PER_CREDIT_USD).toFixed(4)),
    response_metadata: {
      search_count: result.searchCount,
      verified_count: result.verifiedCount,
      no_match_count: result.noMatchCount,
      ...(insertErrors.length ? { insert_errors: insertErrors } : {}),
    },
  }])

  return inserted
}

/**
 * Enrich a single batch item — validate website, Hunter lookup, Prospeo fallback,
 * persist contacts, write enrichment_log + sourced_websites status.
 *
 * @param {{ id: string, website_id: string }} item
 * @param {string} batchId
 * @param {string} domain
 * @param {EnrichmentMetrics} metrics
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function processSingleItem(item, batchId, domain, metrics, logger) {
  await supabase
    .from('sourced_websites')
    .update({ status: 'enriching', updated_at: new Date().toISOString() })
    .eq('id', item.website_id)

  // --- Step 1: Website validation ---
  const validation = await validateWebsite(domain)
  if (!validation.valid) {
    await supabase
      .from('sourced_websites')
      .update({
        status: 'disqualified',
        notes: `Site unreachable: ${validation.reason}`,
        updated_at: new Date().toISOString(),
      })
      .eq('id', item.website_id)

    await supabase.from('enrichment_log').insert([{
      website_id: item.website_id,
      step: 'website_validation',
      success: false,
      contacts_found: 0,
      credits_used: 0,
      cost_usd: 0,
      response_metadata: { reason: validation.reason },
    }])

    metrics.sitesDisqualified++
    metrics.finalStatusCounts.disqualified = (metrics.finalStatusCounts.disqualified ?? 0) + 1
    await markItemDone(batchId, item.id, 'skipped', 0, `Site unreachable: ${validation.reason}`, logger)
    return
  }

  // --- Step 2: Hunter domain search ---
  const hunterUrl = `https://api.hunter.io/v2/domain-search?domain=${encodeURIComponent(domain)}&api_key=${process.env.HUNTER_API_KEY}`
  const response = await fetch(hunterUrl)
  let data
  try {
    data = await response.json()
  } catch {
    throw new Error(`Hunter API ${response.status} ${response.statusText}`)
  }
  if (!response.ok) {
    throw new Error(hunterApiErrorMessage(data, response.status))
  }

  metrics.hunterCreditsUsed++
  const emails = data.data?.emails || []
  if (emails.length > 0) metrics.hunterHits++
  else metrics.hunterMisses++

  // --- Step 3: Persist Hunter contacts ---
  const hunterContactRows = emails.map(hunterEmailToContactRow)
  const { inserted: hunterInserted, insertErrors } = await insertContacts(
    item.website_id,
    hunterContactRows,
    logger,
  )

  await supabase.from('enrichment_log').insert([{
    website_id: item.website_id,
    step: 'hunter',
    success: hunterInserted > 0,
    contacts_found: hunterInserted,
    credits_used: 1,
    cost_usd: 0,
    response_metadata: {
      emails_returned: emails.length,
      ...(insertErrors.length ? { insert_errors: insertErrors } : {}),
    },
  }])

  // --- Step 4: Prospeo fallback if Hunter returned zero emails ---
  let prospeoInserted = 0
  let prospeoRan = false
  if (emails.length === 0 && process.env.PROSPEO_API_KEY) {
    prospeoRan = true
    prospeoInserted = await runProspeoFallback(item.website_id, domain, metrics, logger)
  }

  const totalContacts = hunterInserted + prospeoInserted
  metrics.totalContactsFound += totalContacts
  if (totalContacts > 0) metrics.itemsWithContacts++

  // --- Step 5: Final status on the website ---
  let newStatus
  let failureNote = null
  if (totalContacts > 0) {
    newStatus = 'enriched'
    if (hunterInserted === 0 && prospeoInserted > 0) {
      failureNote = `Enriched via Prospeo fallback (${prospeoInserted} contact${prospeoInserted === 1 ? '' : 's'})`
    }
  } else if (emails.length > 0) {
    newStatus = 'enrichment_storage_failed'
    failureNote = `Hunter returned ${emails.length} email(s) but all DB inserts failed`
  } else {
    newStatus = 'enrichment_failed'
    failureNote = prospeoRan ? 'No emails found via Hunter or Prospeo' : 'No emails found for this domain'
  }

  const statusUpdate = { status: newStatus, updated_at: new Date().toISOString() }
  if (failureNote) statusUpdate.notes = failureNote
  await supabase.from('sourced_websites').update(statusUpdate).eq('id', item.website_id)
  metrics.finalStatusCounts[newStatus] = (metrics.finalStatusCounts[newStatus] ?? 0) + 1

  await markItemDone(batchId, item.id, 'completed', totalContacts, undefined, logger)
}

/**
 * Process one claimed chunk of items sequentially (with a 1s spacing between
 * Hunter calls, matching the existing admin-side pacing — Hunter's rate-limit
 * is strict and we don't want to fight it).
 *
 * @param {string} batchId
 * @param {Array<{ id: string, website_id: string }>} items
 * @param {EnrichmentMetrics} metrics
 * @param {{ isStillRunning: () => Promise<boolean> }} control
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function processChunk(batchId, items, metrics, control, logger) {
  const websiteIds = items.map((i) => i.website_id)
  const { data: websites, error: wsErr } = await supabase
    .from('sourced_websites')
    .select('id, domain')
    .in('id', websiteIds)
  if (wsErr) {
    logger.error({ err: wsErr.message }, '[enrichment-runner] failed to hydrate website domains')
    throw wsErr
  }
  const domainMap = new Map((websites || []).map((w) => [w.id, w.domain]))

  for (const item of items) {
    if (!(await control.isStillRunning())) break

    const domain = domainMap.get(item.website_id)
    if (!domain) {
      await markItemDone(batchId, item.id, 'skipped', 0, 'Website not found', logger)
      metrics.itemsProcessed++
      continue
    }

    try {
      await processSingleItem(item, batchId, domain, metrics, logger)
      metrics.itemsProcessed++
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      /** Prospeo fatals (e.g. insufficient credits) are rethrown from runProspeoFallback — do not log as Hunter. */
      const prospeoFatal = err instanceof Error && /** @type {Error & { fatal?: boolean }} */ (err).fatal === true

      logger.error({ domain, err: message, prospeoFatal }, '[enrichment-runner] item failed')

      await supabase
        .from('sourced_websites')
        .update({
          status: 'enrichment_failed',
          notes: prospeoFatal ? `Prospeo error: ${message}` : `Hunter API error: ${message}`,
          updated_at: new Date().toISOString(),
        })
        .eq('id', item.website_id)

      const errCode = err instanceof Error && 'code' in err ? /** @type {Error & { code?: string }} */ (err).code : undefined
      await supabase.from('enrichment_log').insert([{
        website_id: item.website_id,
        step: prospeoFatal ? 'prospeo' : 'hunter',
        success: false,
        contacts_found: 0,
        credits_used: prospeoFatal ? 0 : 1,
        cost_usd: 0,
        response_metadata: prospeoFatal
          ? { error: message, ...(errCode ? { code: errCode } : {}) }
          : { error: message },
      }])

      metrics.finalStatusCounts.enrichment_failed = (metrics.finalStatusCounts.enrichment_failed ?? 0) + 1
      await markItemDone(batchId, item.id, 'failed', 0, message, logger)
      metrics.itemsProcessed++
    }

    // Rate-limit pacing: 1s between Hunter calls matches prior behaviour.
    await new Promise((r) => setTimeout(r, HUNTER_DELAY_MS))
  }
}

/**
 * Finalize the batch — mark completed/failed, skipping if already terminal.
 * @param {string} batchId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function finalize(batchId, logger) {
  const { data: batch } = await supabase
    .from('enrichment_batches')
    .select('status')
    .eq('id', batchId)
    .maybeSingle()
  if (!batch) return
  if (batch.status === 'failed' || batch.status === 'completed') return

  const { error } = await supabase
    .from('enrichment_batches')
    .update({ status: 'completed', updated_at: new Date().toISOString() })
    .eq('id', batchId)
  if (error) {
    logger.error({ batchId, err: error.message }, '[enrichment-runner] finalize: batch update failed')
    throw error
  }
}

/**
 * Run an enrichment batch end-to-end. Idempotent in-process — a second call for
 * the same batchId while the first is still running is a no-op.
 *
 * @param {string} batchId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
export async function runEnrichmentBatch(batchId, logger) {
  if (!supabaseEnabled) {
    throw new Error('Enrichment runner requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY')
  }
  if (!process.env.HUNTER_API_KEY) {
    throw new Error('Enrichment runner requires HUNTER_API_KEY')
  }

  if (activeBatches.has(batchId)) {
    logger.info({ batchId }, '[enrichment-runner] batch already running in-process — skipping')
    return
  }

  const metrics = newMetrics()
  const handle = {
    status: /** @type {'running' | 'cancelled'} */ ('running'),
    startedAt: Date.now(),
    metrics,
  }
  activeBatches.set(batchId, handle)
  logger.info({ batchId }, '[enrichment-runner] starting batch')

  // Move interrupted → running.
  await supabase
    .from('enrichment_batches')
    .update({ status: 'running', updated_at: new Date().toISOString() })
    .eq('id', batchId)
    .in('status', ['interrupted'])

  const control = {
    isStillRunning: async () => {
      if (handle.status === 'cancelled') return false
      if (await isCancelled(batchId, logger)) {
        handle.status = 'cancelled'
        return false
      }
      return true
    },
  }

  try {
    while (true) {
      if (!(await control.isStillRunning())) {
        logger.warn({ batchId }, '[enrichment-runner] batch cancelled — exiting loop')
        break
      }

      const items = await claimItems(batchId, CLAIM_LIMIT)
      if (items.length === 0) break

      await processChunk(batchId, items, metrics, control, logger)
    }

    await finalize(batchId, logger)

    const elapsedMs = Date.now() - handle.startedAt
    const prospeoCost = Number((metrics.prospeoCredits * PROSPEO_COST_PER_CREDIT_USD).toFixed(4))
    logger.info(
      {
        batchId,
        elapsedMs,
        elapsedSec: Math.round(elapsedMs / 1000),
        itemsProcessed: metrics.itemsProcessed,
        itemsWithContacts: metrics.itemsWithContacts,
        totalContactsFound: metrics.totalContactsFound,
        itemsPerMin:
          elapsedMs > 0 ? Number(((metrics.itemsProcessed * 60_000) / elapsedMs).toFixed(2)) : 0,
        hunter: {
          hits: metrics.hunterHits,
          misses: metrics.hunterMisses,
          creditsUsed: metrics.hunterCreditsUsed,
        },
        prospeo: {
          runs: metrics.prospeoRuns,
          rescues: metrics.prospeoRescues,
          credits: metrics.prospeoCredits,
          estCostUsd: prospeoCost,
        },
        sitesDisqualified: metrics.sitesDisqualified,
        finalStatusCounts: metrics.finalStatusCounts,
      },
      '[enrichment-runner] batch summary',
    )
  } catch (err) {
    logger.error({ batchId, err: err instanceof Error ? err.message : String(err) }, '[enrichment-runner] batch failed')
    await supabase
      .from('enrichment_batches')
      .update({ status: 'failed', updated_at: new Date().toISOString() })
      .eq('id', batchId)
  } finally {
    activeBatches.delete(batchId)
  }
}

/**
 * Cancel an in-flight batch (local + DB). Safe to call multiple times.
 * @param {string} batchId
 */
export async function cancelEnrichmentBatch(batchId) {
  const handle = activeBatches.get(batchId)
  if (handle) handle.status = 'cancelled'

  if (!supabaseEnabled) return
  await supabase
    .from('enrichment_batches')
    .update({ status: 'failed', updated_at: new Date().toISOString() })
    .eq('id', batchId)
}

/**
 * Read-only status snapshot. Merges Supabase state with in-process presence.
 * @param {string} batchId
 */
export async function getEnrichmentBatchStatus(batchId) {
  if (!supabaseEnabled) return { error: 'Supabase not configured' }

  const { data: batch, error } = await supabase
    .from('enrichment_batches')
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
    contacts_found: batch.contacts_found,
    sites_with_contacts: batch.sites_with_contacts,
    remaining: batch.total - batch.processed,
    in_process: !!handle,
    in_process_started_at: handle?.startedAt ?? null,
    updated_at: batch.updated_at,
  }
}

/**
 * Live per-batch metrics while in-process. Returns { in_process: false } after
 * the batch completes — the final summary lives in the service logs.
 * @param {string} batchId
 */
export function getEnrichmentBatchMetrics(batchId) {
  const handle = activeBatches.get(batchId)
  if (!handle) return { in_process: false }
  const elapsedMs = Date.now() - handle.startedAt
  return {
    in_process: true,
    started_at: handle.startedAt,
    elapsed_ms: elapsedMs,
    items_per_min:
      elapsedMs > 0 ? Number(((handle.metrics.itemsProcessed * 60_000) / elapsedMs).toFixed(2)) : 0,
    ...handle.metrics,
  }
}

/**
 * On startup: reclaim orphaned items (from a crashed previous instance) and
 * auto-resume any batches still in `running` state. Mirrors the validation
 * runner's recovery behaviour.
 *
 * @param {import('fastify').FastifyBaseLogger} logger
 */
export async function recoverEnrichmentOnStartup(logger) {
  if (!supabaseEnabled) {
    logger.warn('[enrichment-runner] supabase not configured — skipping startup recovery')
    return
  }

  try {
    const { data: reclaimed } = await supabase.rpc('reclaim_stale_enrichment_items', {
      p_stale_seconds: 60,
    })
    if (reclaimed && reclaimed > 0) {
      logger.info({ reclaimed }, '[enrichment-runner] reclaimed stale items on startup')
    }

    const { data: resumable } = await supabase
      .from('enrichment_batches')
      .select('id')
      .in('status', ['running', 'interrupted'])

    for (const batch of resumable || []) {
      logger.info({ batchId: batch.id }, '[enrichment-runner] auto-resuming batch on startup')
      runEnrichmentBatch(batch.id, logger).catch((err) => {
        logger.error({ batchId: batch.id, err: err.message }, '[enrichment-runner] auto-resume failed')
      })
    }
  } catch (err) {
    logger.error(
      { err: err instanceof Error ? err.message : String(err) },
      '[enrichment-runner] startup recovery failed',
    )
  }
}
