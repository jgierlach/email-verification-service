import { supabase, supabaseEnabled } from './supabase.js'

const CLAIM_LIMIT = 20
const PAGE_WAIT_MS = 2000          // Google requires ~2s before using nextPageToken
const INTER_QUERY_PACE_MS = 400    // Small spacing to avoid Places API burst-rate limits

const TEXT_SEARCH_FIELD_MASK = [
  'places.id',
  'places.displayName',
  'places.formattedAddress',
  'places.rating',
  'places.userRatingCount',
  'places.websiteUri',
  'places.nationalPhoneNumber',
  'places.types',
  'nextPageToken',
].join(',')

const TRAILING_COUNTRY = /^(USA|US|United States(?: of America)?)$/i

/**
 * @typedef {object} SourcingMetrics
 * @property {number} queriesProcessed
 * @property {number} queriesFailed
 * @property {number} placesReturned - raw count from Google (includes dupes)
 * @property {number} websitesInserted
 * @property {number} websitesUpdated
 * @property {number} placesWithoutWebsite
 */

/**
 * @type {Map<string, { status: 'running' | 'cancelled', startedAt: number, metrics: SourcingMetrics }>}
 */
const activeJobs = new Map()

/** @returns {SourcingMetrics} */
function newMetrics() {
  return {
    queriesProcessed: 0,
    queriesFailed: 0,
    placesReturned: 0,
    websitesInserted: 0,
    websitesUpdated: 0,
    placesWithoutWebsite: 0,
  }
}

/** @param {string} url */
function extractDomain(url) {
  try {
    const full = url.startsWith('http') ? url : `https://${url}`
    return new URL(full).hostname.replace(/^www\./, '')
  } catch {
    return null
  }
}

/**
 * Parse city, state, ZIP from a Google-style formatted address.
 * @param {string} address
 * @returns {{ city: string, state: string, zip: string }}
 */
function parseAddress(address) {
  if (!address) return { city: '', state: '', zip: '' }
  let parts = address.split(',').map((p) => p.trim()).filter(Boolean)
  while (parts.length > 1 && TRAILING_COUNTRY.test(parts[parts.length - 1])) {
    parts = parts.slice(0, -1)
  }
  if (parts.length < 2) return { city: '', state: '', zip: '' }
  const city = parts[parts.length - 2]
  const stateZip = parts[parts.length - 1]
  const stateMatch = stateZip.match(/([A-Za-z]{2})\s*(\d{5}(?:-\d{4})?)?/)
  return {
    city: city || '',
    state: stateMatch?.[1] ? stateMatch[1].toUpperCase() : '',
    zip: stateMatch?.[2] || '',
  }
}

/**
 * @param {string} textQuery
 * @param {string | null} pageToken
 */
async function searchPlaces(textQuery, pageToken = null) {
  const body = { textQuery, languageCode: 'en', regionCode: 'US', pageSize: 20 }
  if (pageToken) body.pageToken = pageToken

  const res = await fetch('https://places.googleapis.com/v1/places:searchText', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Api-Key': process.env.GOOGLE_PLACES_API_KEY,
      'X-Goog-FieldMask': TEXT_SEARCH_FIELD_MASK,
    },
    body: JSON.stringify(body),
  })

  let data
  try {
    data = await res.json()
  } catch {
    throw new Error(`Places API ${res.status} ${res.statusText}`)
  }

  if (!res.ok) {
    const err = data?.error
    const detail =
      err && typeof err === 'object' && err.message
        ? err.message
        : typeof err === 'string'
          ? err
          : JSON.stringify(err || data)
    throw new Error(`Places API ${res.status}: ${detail}`)
  }

  if (data.error) {
    throw new Error(`Places API error: ${data.error.message || JSON.stringify(data.error)}`)
  }

  return { places: data.places || [], nextPageToken: data.nextPageToken || null }
}

/**
 * Run one sourcing query through Places (up to 3 pages) and upsert websites.
 * Returns per-query counts for the item row + metrics tracking.
 *
 * @param {{ query: string, vertical: string | null, city: string | null, state: string | null }} item
 * @param {SourcingMetrics} metrics
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function processSingleQuery(item, metrics, logger) {
  let totalFound = 0
  let totalNew = 0
  let nextPageToken = null
  let pageCount = 0

  do {
    if (nextPageToken) {
      await new Promise((r) => setTimeout(r, PAGE_WAIT_MS))
    }

    const result = await searchPlaces(item.query, nextPageToken)
    totalFound += result.places.length
    metrics.placesReturned += result.places.length

    for (const place of result.places) {
      const website = place.websiteUri
      if (!website) {
        metrics.placesWithoutWebsite++
        continue
      }
      const domain = extractDomain(website)
      if (!domain) continue

      const addressParts = parseAddress(place.formattedAddress)

      const { data: existing } = await supabase
        .from('sourced_websites')
        .select('id')
        .eq('domain', domain)
        .maybeSingle()

      if (existing) {
        const { error } = await supabase
          .from('sourced_websites')
          .update({
            phone: place.nationalPhoneNumber || undefined,
            address: place.formattedAddress || undefined,
            city: addressParts.city || undefined,
            state: addressParts.state || undefined,
            zip: addressParts.zip || undefined,
            google_rating: place.rating ?? undefined,
            google_review_count: place.userRatingCount ?? undefined,
            google_place_id: place.id || undefined,
            business_category: place.types?.[0] || undefined,
            updated_at: new Date().toISOString(),
          })
          .eq('id', existing.id)
        if (error) {
          logger.warn({ domain, err: error.message }, '[sourcing-runner] existing website update failed')
        } else {
          metrics.websitesUpdated++
        }
        continue
      }

      const { error: insertError } = await supabase
        .from('sourced_websites')
        .insert([{
          url: website,
          domain,
          business_name: place.displayName?.text || null,
          address: place.formattedAddress || null,
          city: addressParts.city || item.city,
          state: addressParts.state || item.state,
          zip: addressParts.zip || null,
          phone: place.nationalPhoneNumber || null,
          google_rating: place.rating || null,
          google_review_count: place.userRatingCount || null,
          google_place_id: place.id,
          business_category: place.types?.[0] || null,
          vertical: item.vertical,
          source: 'google_places',
          source_metadata: { query: item.query, types: place.types },
        }])

      if (!insertError) {
        totalNew++
        metrics.websitesInserted++
      } else {
        // Most likely a unique-constraint race (domain already exists) — don't log noisily.
        if (insertError.code !== '23505') {
          logger.warn({ domain, err: insertError.message }, '[sourcing-runner] website insert failed')
        }
      }
    }

    nextPageToken = result.nextPageToken
    pageCount++
  } while (nextPageToken && pageCount < 3)

  return { totalFound, totalNew }
}

/**
 * Claim a chunk of pending items for this job.
 * @param {string} jobId
 * @param {number} limit
 * @returns {Promise<Array<{ id: string, query: string, vertical: string|null, city: string|null, state: string|null }>>}
 */
async function claimItems(jobId, limit) {
  const { data, error } = await supabase.rpc('claim_sourcing_job_items', {
    p_job_id: jobId,
    p_limit: limit,
  })
  if (error) throw error
  return data || []
}

/**
 * Cancellation check (admin cancel endpoint flips status to `failed`).
 * @param {string} jobId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function isCancelled(jobId, logger) {
  const { data, error } = await supabase
    .from('sourcing_jobs')
    .select('status')
    .eq('id', jobId)
    .maybeSingle()
  if (error) {
    logger.error({ jobId, err: error.message }, '[sourcing-runner] isCancelled poll failed')
    throw error
  }
  return data?.status === 'failed' || data?.status === 'cancelled'
}

/**
 * Bump job counters atomically. Doubles as heartbeat.
 * @param {string} jobId
 * @param {{ found?: number, new?: number, status?: string }} updates
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function bumpJobCounters(jobId, updates, logger) {
  const { error } = await supabase.rpc('increment_sourcing_job_progress', {
    p_job_id: jobId,
    p_found: updates.found ?? 0,
    p_new: updates.new ?? 0,
    p_status: updates.status ?? null,
  })
  if (error) {
    logger.error({ jobId, err: error.message }, '[sourcing-runner] increment_sourcing_job_progress failed')
  }
}

/**
 * Finalize the job — only if no pending/processing items remain.
 * @param {string} jobId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
async function finalize(jobId, logger) {
  const { data: job } = await supabase
    .from('sourcing_jobs')
    .select('status')
    .eq('id', jobId)
    .maybeSingle()
  if (!job) return
  if (job.status === 'failed' || job.status === 'completed') return

  const { error } = await supabase
    .from('sourcing_jobs')
    .update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
    .eq('id', jobId)
  if (error) {
    logger.error({ jobId, err: error.message }, '[sourcing-runner] finalize failed')
  }
}

/**
 * Run a sourcing job end-to-end. Idempotent in-process.
 *
 * @param {string} jobId
 * @param {import('fastify').FastifyBaseLogger} logger
 */
export async function runSourcingJob(jobId, logger) {
  if (!supabaseEnabled) {
    throw new Error('Sourcing runner requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY')
  }
  if (!process.env.GOOGLE_PLACES_API_KEY) {
    throw new Error('Sourcing runner requires GOOGLE_PLACES_API_KEY')
  }

  if (activeJobs.has(jobId)) {
    logger.info({ jobId }, '[sourcing-runner] job already running in-process — skipping')
    return
  }

  const metrics = newMetrics()
  const handle = {
    status: /** @type {'running' | 'cancelled'} */ ('running'),
    startedAt: Date.now(),
    metrics,
  }
  activeJobs.set(jobId, handle)
  logger.info({ jobId }, '[sourcing-runner] starting job')

  // Flip interrupted → running so later status queries don't see stale.
  await supabase
    .from('sourcing_jobs')
    .update({ status: 'running', updated_at: new Date().toISOString() })
    .eq('id', jobId)
    .in('status', ['interrupted'])

  try {
    while (true) {
      if (handle.status === 'cancelled' || (await isCancelled(jobId, logger))) {
        logger.warn({ jobId }, '[sourcing-runner] job cancelled — exiting loop')
        break
      }

      const items = await claimItems(jobId, CLAIM_LIMIT)
      if (items.length === 0) break

      for (const item of items) {
        if (handle.status === 'cancelled') break

        try {
          const { totalFound, totalNew } = await processSingleQuery(item, metrics, logger)

          const { error: itemErr } = await supabase
            .from('sourcing_job_items')
            .update({
              status: 'completed',
              found: totalFound,
              new_count: totalNew,
              updated_at: new Date().toISOString(),
            })
            .eq('id', item.id)
          if (itemErr) {
            logger.error({ itemId: item.id, err: itemErr.message }, '[sourcing-runner] item update failed')
          }

          await bumpJobCounters(jobId, { found: totalFound, new: totalNew }, logger)
          metrics.queriesProcessed++

          logger.info(
            { jobId, query: item.query, found: totalFound, new: totalNew },
            '[sourcing-runner] query complete',
          )
        } catch (err) {
          logger.error(
            { jobId, query: item.query, err: err instanceof Error ? err.message : String(err) },
            '[sourcing-runner] query failed',
          )
          await supabase
            .from('sourcing_job_items')
            .update({
              status: 'failed',
              error_message: err instanceof Error ? err.message : String(err),
              updated_at: new Date().toISOString(),
            })
            .eq('id', item.id)

          // Still heartbeat so staleness detection sees progress.
          await bumpJobCounters(jobId, {}, logger)
          metrics.queriesFailed++
        }

        // Tiny pacing between queries to avoid burst-rate limits on Places.
        await new Promise((r) => setTimeout(r, INTER_QUERY_PACE_MS))
      }
    }

    await finalize(jobId, logger)

    const elapsedMs = Date.now() - handle.startedAt
    logger.info(
      {
        jobId,
        elapsedMs,
        elapsedSec: Math.round(elapsedMs / 1000),
        queriesPerMin:
          elapsedMs > 0
            ? Number(((metrics.queriesProcessed * 60_000) / elapsedMs).toFixed(2))
            : 0,
        ...metrics,
      },
      '[sourcing-runner] job summary',
    )
  } catch (err) {
    logger.error(
      { jobId, err: err instanceof Error ? err.message : String(err) },
      '[sourcing-runner] job failed',
    )
    await supabase
      .from('sourcing_jobs')
      .update({ status: 'failed', updated_at: new Date().toISOString() })
      .eq('id', jobId)
  } finally {
    activeJobs.delete(jobId)
  }
}

/**
 * Cancel an in-flight sourcing job.
 * @param {string} jobId
 */
export async function cancelSourcingJob(jobId) {
  const handle = activeJobs.get(jobId)
  if (handle) handle.status = 'cancelled'

  if (!supabaseEnabled) return
  await supabase
    .from('sourcing_jobs')
    .update({ status: 'failed', updated_at: new Date().toISOString() })
    .eq('id', jobId)
}

/**
 * Status snapshot: Supabase row + in-process presence.
 * @param {string} jobId
 */
export async function getSourcingJobStatus(jobId) {
  if (!supabaseEnabled) return { error: 'Supabase not configured' }

  const { data: job, error } = await supabase
    .from('sourcing_jobs')
    .select('*')
    .eq('id', jobId)
    .maybeSingle()
  if (error) return { error: error.message }
  if (!job) return { error: 'Job not found' }

  const handle = activeJobs.get(jobId)
  return {
    job_id: job.id,
    status: job.status,
    total_found: job.total_found,
    total_new: job.total_new,
    in_process: !!handle,
    in_process_started_at: handle?.startedAt ?? null,
    updated_at: job.updated_at,
    completed_at: job.completed_at,
  }
}

/**
 * Live per-job metrics while in-process.
 * @param {string} jobId
 */
export function getSourcingJobMetrics(jobId) {
  const handle = activeJobs.get(jobId)
  if (!handle) return { in_process: false }
  const elapsedMs = Date.now() - handle.startedAt
  return {
    in_process: true,
    started_at: handle.startedAt,
    elapsed_ms: elapsedMs,
    queries_per_min:
      elapsedMs > 0
        ? Number(((handle.metrics.queriesProcessed * 60_000) / elapsedMs).toFixed(2))
        : 0,
    ...handle.metrics,
  }
}

/**
 * On startup, reclaim orphaned items and auto-resume any in-flight jobs.
 * @param {import('fastify').FastifyBaseLogger} logger
 */
export async function recoverSourcingOnStartup(logger) {
  if (!supabaseEnabled) {
    logger.warn('[sourcing-runner] supabase not configured — skipping startup recovery')
    return
  }

  try {
    const { data: reclaimed } = await supabase.rpc('reclaim_stale_sourcing_items', {
      p_stale_seconds: 60,
    })
    if (reclaimed && reclaimed > 0) {
      logger.info({ reclaimed }, '[sourcing-runner] reclaimed stale items on startup')
    }

    const { data: resumable } = await supabase
      .from('sourcing_jobs')
      .select('id')
      .in('status', ['running', 'interrupted'])

    for (const job of resumable || []) {
      logger.info({ jobId: job.id }, '[sourcing-runner] auto-resuming job on startup')
      runSourcingJob(job.id, logger).catch((err) => {
        logger.error(
          { jobId: job.id, err: err instanceof Error ? err.message : String(err) },
          '[sourcing-runner] auto-resume failed',
        )
      })
    }
  } catch (err) {
    logger.error(
      { err: err instanceof Error ? err.message : String(err) },
      '[sourcing-runner] startup recovery failed',
    )
  }
}
