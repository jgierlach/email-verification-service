const SEARCH_URL = 'https://api.apollo.io/api/v1/mixed_people/api_search'
const BULK_MATCH_URL = 'https://api.apollo.io/api/v1/people/bulk_match'

// Apollo accepts these lowercase seniority tokens. Override at runtime with
// APOLLO_SENIORITIES=csv,list. Owner/founder/c_suite are the highest-intent
// outreach targets; manager is kept as a floor for small-org decision makers.
const DEFAULT_SENIORITIES = (process.env.APOLLO_SENIORITIES || 'owner,founder,c_suite,partner,vp,head,director,manager')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean)

// Hard cap on email reveals per domain. Apollo bills per revealed email —
// revealing more than a handful of contacts at one company rarely improves
// outbound results and burns credits fast.
const MAX_REVEALS_PER_DOMAIN = Number(process.env.APOLLO_MAX_REVEALS_PER_DOMAIN || 5)

// Apollo plans meter usage in "credits" but the dashboard is the source of
// truth. This is a nominal stamp for cost estimation in logs — tune to match
// your plan's effective $/credit.
export const APOLLO_COST_PER_CREDIT_USD = Number(process.env.APOLLO_COST_PER_CREDIT_USD || 0.04)

/** @param {number} attempt zero-indexed */
function backoffMs(attempt) {
  return 2000 * Math.pow(2, attempt)
}

/** @param {number} ms */
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

/**
 * Pull the handful of response headers Apollo uses for rate-limit / credit
 * reporting. Header presence depends on plan and endpoint — we best-effort
 * capture whatever they send so the diagnostic logs can show remaining usage.
 *
 * @param {Headers} headers
 */
function extractApolloUsageHeaders(headers) {
  const keys = [
    'x-rate-limit-minute',
    'x-minute-requests-left',
    'x-rate-limit-hourly',
    'x-hourly-requests-left',
    'x-rate-limit-24-hour',
    'x-24-hour-requests-left',
    'x-rate-limit-remaining',
    'x-rate-limit-credit-price',
  ]
  /** @type {Record<string, string>} */
  const out = {}
  for (const k of keys) {
    const v = headers.get(k)
    if (v != null) out[k] = v
  }
  return out
}

/**
 * POST JSON to an Apollo endpoint with retry + fatal-error detection.
 * Throws a `{ fatal: true, code }` error on insufficient credits / bad key,
 * matching the Prospeo client's contract so enrichment-runner can stop the
 * batch the same way.
 *
 * @param {string} url
 * @param {object} body
 * @returns {Promise<{ data: any, usage: Record<string, string> }>}
 */
async function apolloFetch(url, body) {
  if (!process.env.APOLLO_API_KEY) {
    throw new Error('APOLLO_API_KEY not configured')
  }

  const maxRetries = 3
  let lastError

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    let response
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          'X-Api-Key': process.env.APOLLO_API_KEY,
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache',
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
      throw new Error(`Apollo API ${response.status} ${response.statusText}`)
    }

    if (response.status === 429) {
      if (attempt < maxRetries) {
        await sleep(backoffMs(attempt))
        continue
      }
      throw new Error('Apollo rate limited — exhausted retries')
    }

    if (response.status === 402 || response.status === 403) {
      /** @type {Error & { fatal?: boolean, code?: string }} */
      const err = new Error(`Apollo fatal: HTTP ${response.status} ${data?.error || data?.message || ''}`.trim())
      err.fatal = true
      err.code = response.status === 402 ? 'INSUFFICIENT_CREDITS' : 'FORBIDDEN'
      throw err
    }

    if (response.status === 401) {
      /** @type {Error & { fatal?: boolean, code?: string }} */
      const err = new Error('Apollo fatal: invalid API key')
      err.fatal = true
      err.code = 'INVALID_API_KEY'
      throw err
    }

    if (!response.ok) {
      throw new Error(`Apollo API ${response.status}: ${data?.error || data?.message || response.statusText}`)
    }

    // Apollo sometimes returns 200 with an `error` string (e.g. endpoint
    // gated by plan). Treat credit-related strings as fatal.
    if (data?.error && /credit|limit|quota/i.test(String(data.error))) {
      /** @type {Error & { fatal?: boolean, code?: string }} */
      const err = new Error(`Apollo fatal: ${data.error}`)
      err.fatal = true
      err.code = 'INSUFFICIENT_CREDITS'
      throw err
    }

    return { data, usage: extractApolloUsageHeaders(response.headers) }
  }

  throw lastError || new Error('Apollo request failed')
}

/**
 * Search Apollo's people database by company domain. Search itself does not
 * consume email credits — it only returns metadata with obfuscated emails.
 *
 * @param {string} domain
 * @param {{ seniorities?: string[], perPage?: number }} [options]
 * @returns {Promise<{ people: any[] }>}
 */
export async function apolloSearchPeopleByDomain(domain, options = {}) {
  const seniorities = options.seniorities ?? DEFAULT_SENIORITIES
  const perPage = options.perPage ?? Math.max(MAX_REVEALS_PER_DOMAIN * 2, 10)

  /** @type {Record<string, unknown>} */
  const body = {
    q_organization_domains_list: [domain],
    page: 1,
    per_page: perPage,
  }
  if (seniorities.length > 0) body.person_seniorities = seniorities

  const { data, usage } = await apolloFetch(SEARCH_URL, body)

  const people = Array.isArray(data?.people) ? data.people : []
  return { people, usage, rawPagination: data?.pagination ?? null }
}

/**
 * Reveal work emails for up to N Apollo people in a single call. Bills 1
 * credit per email actually revealed. Candidates with `email_status` of
 * `unavailable` / `not_found` should be filtered before this call — Apollo
 * may still attempt (and sometimes bill) on those.
 *
 * @param {any[]} candidates - items from apolloSearchPeopleByDomain().people
 * @param {string} domain
 */
export async function apolloBulkMatch(candidates, domain) {
  if (candidates.length === 0) return { matches: [], usage: {}, raw: null }

  const details = candidates.map((p) => ({
    id: p.id,
    first_name: p.first_name,
    last_name: p.last_name,
    organization_name: p.organization?.name,
    domain,
  }))

  const { data, usage } = await apolloFetch(BULK_MATCH_URL, {
    reveal_personal_emails: false,
    reveal_phone_number: false,
    details,
  })

  const matches = Array.isArray(data?.matches) ? data.matches : []
  return { matches, usage, raw: data }
}

/** @param {string | null | undefined} title */
function isDecisionMakerTitle(title) {
  if (!title) return false
  const lower = title.toLowerCase()
  return [
    'owner', 'founder', 'ceo', 'executive director', 'pastor', 'senior pastor',
    'president', 'managing partner', 'administrator', 'director', 'principal',
    'manager', 'partner', 'chief', 'vp', 'vice president', 'head of',
  ].some((t) => lower.includes(t))
}

/** @param {string | null | undefined} email */
function isRevealedEmail(email) {
  if (!email) return false
  const lower = email.toLowerCase()
  if (lower.includes('email_not_unlocked')) return false
  if (lower.includes('domain.com') && lower.startsWith('email_')) return false
  if (!lower.includes('@')) return false
  return true
}

/**
 * Build a sourced_contacts row from an Apollo match/search object. Caller
 * fills in website_id.
 *
 * @param {any} person - shape from either bulk_match.matches[] or search.people[]
 */
function buildContactRow(person) {
  const firstName = person.first_name || null
  const lastName = person.last_name || null
  const email = person.email || person.personal_emails?.[0] || null
  const title = person.title || person.headline || null

  return {
    first_name: firstName,
    last_name: lastName,
    name: person.name || [firstName, lastName].filter(Boolean).join(' ') || null,
    title,
    email,
    phone: null,
    source: 'apollo',
    source_metadata: {
      apollo_id: person.id || null,
      linkedin_url: person.linkedin_url || null,
      organization_name: person.organization?.name || null,
      organization_domain: person.organization?.primary_domain || null,
      seniority: person.seniority || null,
      email_status: person.email_status || null,
      confidence: person.extrapolated_email_confidence ?? null,
    },
    verification_status: 'pending',
    is_decision_maker: isDecisionMakerTitle(title),
  }
}

/**
 * Fallback orchestrator. Runs after Hunter + Prospeo both fail to produce
 * contacts for a domain. Two-round search: seniority-preferred, then loosened.
 * Reveals emails via one `bulk_match` call, up to MAX_REVEALS_PER_DOMAIN.
 *
 * @param {string} domain
 * @param {import('fastify').FastifyBaseLogger | typeof console} [logger]
 * @returns {Promise<{
 *   contacts: object[],
 *   creditsUsed: number,
 *   candidatesFound: number,
 *   revealsAttempted: number,
 *   seniorityRoundHit: boolean,
 * }>}
 */
export async function apolloFallbackEnrich(domain, logger = console) {
  // --- Round 1: search with seniority filter ---
  let people = []
  let seniorityRoundHit = false
  /** @type {Record<string, string>} */
  let lastUsage = {}

  try {
    const senior = await apolloSearchPeopleByDomain(domain)
    people = senior.people
    lastUsage = senior.usage || {}
    seniorityRoundHit = people.length > 0
    logger.debug({ domain, count: people.length, usage: lastUsage }, '[apollo] round1 search done')
  } catch (err) {
    if (err.fatal) throw err
    logger.error({ domain, err: err.message }, '[apollo] round1 search errored')
  }

  // --- Round 2: widen if round 1 returned nothing ---
  if (people.length === 0) {
    try {
      const loose = await apolloSearchPeopleByDomain(domain, { seniorities: [] })
      people = loose.people
      lastUsage = loose.usage || lastUsage
      logger.debug({ domain, count: people.length, usage: lastUsage }, '[apollo] round2 search done')
    } catch (err) {
      if (err.fatal) throw err
      logger.error({ domain, err: err.message }, '[apollo] round2 search errored')
    }
  }

  if (people.length === 0) {
    logger.info({ domain }, '[apollo] no candidates found across both rounds')
    return {
      contacts: [],
      creditsUsed: 0,
      candidatesFound: 0,
      revealsAttempted: 0,
      seniorityRoundHit: false,
    }
  }

  // Filter out candidates whose email Apollo already said is unavailable —
  // no point burning a credit on a lookup that won't produce an email.
  const revealable = people
    .filter((p) => p.email_status !== 'unavailable' && p.email_status !== 'not_found')
    .slice(0, MAX_REVEALS_PER_DOMAIN)

  // Summarize candidate email_status distribution so we can see if Apollo is
  // consistently returning only `unavailable` for a domain (= no reveal value
  // even at max plan) vs returning real leads.
  /** @type {Record<string, number>} */
  const candidateStatuses = {}
  for (const p of people) {
    const s = p.email_status || 'null'
    candidateStatuses[s] = (candidateStatuses[s] || 0) + 1
  }

  if (revealable.length === 0) {
    logger.info(
      { domain, candidates: people.length, candidateStatuses },
      '[apollo] candidates found but all had unavailable email_status',
    )
    return {
      contacts: [],
      creditsUsed: 0,
      candidatesFound: people.length,
      revealsAttempted: 0,
      seniorityRoundHit,
    }
  }

  logger.info(
    { domain, candidates: people.length, revealing: revealable.length, candidateStatuses, seniorityRoundHit, usage: lastUsage },
    '[apollo] revealing emails via bulk_match',
  )

  const { matches, usage: matchUsage, raw: matchRaw } = await apolloBulkMatch(revealable, domain)

  const contacts = []
  /** @type {Array<{ has_email: boolean, email_looks_unlocked: boolean, email_status: string | null, email_sample: string | null }>} */
  const matchDiagnostics = []
  for (const match of matches) {
    const hasEmail = !!match?.email
    const looksUnlocked = hasEmail && isRevealedEmail(match.email)
    matchDiagnostics.push({
      has_email: hasEmail,
      email_looks_unlocked: looksUnlocked,
      email_status: match?.email_status ?? null,
      // Sample only the part before @ so we don't log full PII at scale,
      // and mask obvious obfuscation markers so the reason is visible.
      email_sample: hasEmail ? String(match.email).replace(/@.+$/, '@…') : null,
    })

    if (!looksUnlocked) continue
    contacts.push(buildContactRow(match))
  }

  // Apollo bills per email actually revealed. If Apollo returned `matches`
  // but none had revealed emails (plan-gated or partial match), we want to
  // see exactly what the response looked like so we can decide whether to
  // keep calling bulk_match for this tier of leads.
  const creditsUsed = contacts.length

  if (contacts.length === 0 && matches.length > 0) {
    logger.warn(
      {
        domain,
        candidates: people.length,
        revealing: revealable.length,
        matchesReturned: matches.length,
        candidateStatuses,
        matchDiagnostics,
        matchUsage,
        // Top-level shape only — avoid dumping full PII payloads.
        rawKeys: matchRaw ? Object.keys(matchRaw) : null,
        rawError: matchRaw?.error ?? null,
        rawMessage: matchRaw?.message ?? null,
      },
      '[apollo] bulk_match returned matches but no revealed emails — inspect response',
    )
  }

  logger.info(
    { domain, candidates: people.length, revealed: contacts.length, creditsUsed, usage: matchUsage },
    '[apollo] domain done',
  )

  return {
    contacts,
    creditsUsed,
    candidatesFound: people.length,
    revealsAttempted: revealable.length,
    seniorityRoundHit,
  }
}
