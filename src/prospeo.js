const SEARCH_URL = 'https://api.prospeo.io/search-person'
const ENRICH_URL = 'https://api.prospeo.io/enrich-person'

// Prospeo's taxonomy rejects 'VP' with HTTP 400 — use the spelled-out form.
// Override at runtime with PROSPEO_SENIORITIES=csv,list.
const DEFAULT_SENIORITIES = (process.env.PROSPEO_SENIORITIES || 'Founder/Owner,C-Suite,Vice President,Director')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean)

// Prospeo pricing (as of April 2026): ~$0.03 per credit on entry plans.
export const PROSPEO_COST_PER_CREDIT_USD = 0.03

/** @param {number} attempt zero-indexed */
function backoffMs(attempt) {
  return 2000 * Math.pow(2, attempt)
}

/** @param {number} ms */
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

/**
 * POST JSON to a Prospeo endpoint with retry logic for 429 and INTERNAL_ERROR.
 * Throws on fatal errors (INSUFFICIENT_CREDITS, INVALID_API_KEY).
 *
 * @param {string} url
 * @param {object} body
 * @returns {Promise<any>}
 */
async function prospeoFetch(url, body) {
  if (!process.env.PROSPEO_API_KEY) {
    throw new Error('PROSPEO_API_KEY not configured')
  }

  const maxRetries = 3
  let lastError

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    let response
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          'X-KEY': process.env.PROSPEO_API_KEY,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      })
    } catch (err) {
      lastError = err
      if (attempt < maxRetries) {
        await sleep(backoffMs(attempt))
        continue
      }
      throw err
    }

    let data
    try {
      data = await response.json()
    } catch {
      throw new Error(`Prospeo API ${response.status} ${response.statusText}`)
    }

    if (response.status === 429) {
      if (attempt < maxRetries) {
        await sleep(backoffMs(attempt))
        continue
      }
      throw new Error('Prospeo rate limited — exhausted retries')
    }

    if (data?.error) {
      const code = data.error_code
      if (code === 'INSUFFICIENT_CREDITS' || code === 'INVALID_API_KEY') {
        /** @type {Error & { fatal?: boolean, code?: string }} */
        const err = new Error(`Prospeo fatal: ${code}`)
        err.fatal = true
        err.code = code
        throw err
      }
      if (code === 'INTERNAL_ERROR') {
        if (attempt < maxRetries) {
          await sleep(backoffMs(attempt))
          continue
        }
        throw new Error('Prospeo internal error — exhausted retries')
      }
      return data
    }

    if (!response.ok) {
      throw new Error(`Prospeo API ${response.status} ${response.statusText}`)
    }

    return data
  }

  throw lastError || new Error('Prospeo request failed')
}

/**
 * Search for people at a company by domain. Page 1 only.
 * @param {string} domain
 * @param {{ seniorities?: string[], page?: number }} [options]
 * @returns {Promise<{ results: any[], creditsUsed: number }>}
 */
export async function prospeoSearchByDomain(domain, options = {}) {
  const seniorities = options.seniorities ?? DEFAULT_SENIORITIES
  const page = options.page ?? 1

  const data = await prospeoFetch(SEARCH_URL, {
    page,
    filters: {
      company: { websites: { include: [domain] } },
      person_seniority: { include: seniorities },
    },
  })

  if (data?.error) {
    if (data.error_code === 'NO_RESULTS') {
      return { results: [], creditsUsed: 0 }
    }
    throw new Error(`Prospeo search failed: ${data.error_code || 'unknown'}`)
  }

  const results = Array.isArray(data?.results) ? data.results : []
  return { results, creditsUsed: results.length > 0 ? 1 : 0 }
}

/**
 * Enrich a single person by person_id. Returns null on NO_MATCH.
 * @param {string} personId
 * @returns {Promise<{ data: any | null, creditsUsed: number }>}
 */
export async function prospeoEnrichPerson(personId) {
  const data = await prospeoFetch(ENRICH_URL, {
    only_verified_email: true,
    data: { person_id: personId },
  })

  if (data?.error) {
    if (data.error_code === 'NO_MATCH') {
      return { data: null, creditsUsed: 0 }
    }
    throw new Error(`Prospeo enrich failed: ${data.error_code || 'unknown'}`)
  }

  const creditsUsed = data?.free_enrichment ? 0 : 1
  return { data, creditsUsed }
}

/**
 * Convert a Prospeo enrich response into a row ready for sourced_contacts insert.
 * Caller fills in website_id.
 */
function buildContactRow(enrichResponse, searchResult) {
  const person = enrichResponse.person || {}
  const company = enrichResponse.company || {}
  const searchPerson = searchResult?.person || {}
  const searchCompany = searchResult?.company || {}

  return {
    first_name: person.first_name || null,
    last_name: person.last_name || null,
    name: person.full_name || [person.first_name, person.last_name].filter(Boolean).join(' ') || null,
    title: person.current_job_title || null,
    email: person.email?.email || null,
    phone: null,
    source: 'prospeo',
    source_metadata: {
      person_id: person.person_id || searchPerson.person_id,
      linkedin_url: searchPerson.linkedin_url || null,
      company_name: company.name || searchCompany.name || null,
      industry: company.industry || searchCompany.industry || null,
      employee_range: company.employee_range || searchCompany.employee_range || null,
      verification_method: person.email?.verification_method || null,
      email_mx_provider: person.email?.email_mx_provider || null,
      raw: enrichResponse,
    },
    verification_status: 'pending',
    is_decision_maker: true,
  }
}

/**
 * Fallback orchestrator. Runs after Hunter returns zero emails for a domain.
 *
 * @param {string} domain
 * @param {import('fastify').FastifyBaseLogger | typeof console} [logger]
 * @returns {Promise<{
 *   contacts: object[],
 *   creditsUsed: number,
 *   searchCount: number,
 *   verifiedCount: number,
 *   noMatchCount: number,
 * }>}
 */
export async function prospeoFallbackEnrich(domain, logger = console) {
  const { results, creditsUsed: searchCredits } = await prospeoSearchByDomain(domain)

  if (!results.length) {
    logger.info({ domain }, '[enrichment-runner] prospeo search — NO_RESULTS')
    return { contacts: [], creditsUsed: 0, searchCount: 0, verifiedCount: 0, noMatchCount: 0 }
  }

  logger.info({ domain, count: results.length }, '[enrichment-runner] prospeo search — found contacts')

  const contacts = []
  let enrichCredits = 0
  let noMatchCount = 0

  for (const result of results) {
    const personId = result?.person?.person_id
    if (!personId) continue

    const fullName = result.person.full_name || `${result.person.first_name || ''} ${result.person.last_name || ''}`.trim()

    try {
      const { data, creditsUsed } = await prospeoEnrichPerson(personId)
      enrichCredits += creditsUsed

      if (!data) {
        noMatchCount++
        logger.info({ domain, fullName }, '[enrichment-runner] prospeo enrich — NO_MATCH')
        continue
      }

      if (data.person?.email?.status !== 'VERIFIED' || !data.person?.email?.email) {
        noMatchCount++
        continue
      }

      contacts.push(buildContactRow(data, result))
      logger.info({ domain, fullName, email: data.person.email.email }, '[enrichment-runner] prospeo enrich — VERIFIED')
    } catch (err) {
      if (err.fatal) throw err
      logger.error(
        { domain, fullName, err: err.message },
        '[enrichment-runner] prospeo enrich — error',
      )
    }

    await sleep(1000)
  }

  logger.info(
    { domain, searched: results.length, verified: contacts.length, noMatch: noMatchCount },
    '[enrichment-runner] prospeo domain done',
  )

  return {
    contacts,
    creditsUsed: searchCredits + enrichCredits,
    searchCount: results.length,
    verifiedCount: contacts.length,
    noMatchCount,
  }
}
