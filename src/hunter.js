const DOMAIN_SEARCH_URL = 'https://api.hunter.io/v2/domain-search'

/** @param {number} attempt zero-indexed */
function backoffMs(attempt) {
  return 2000 * Math.pow(2, attempt)
}

/** @param {number} ms */
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

/**
 * Parse a Hunter API error response into a useful message. Hunter returns
 * `{ errors: [{ id, details }] }` on failures.
 *
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
 * @typedef {{ success: true, data: any }} HunterSuccess
 * @typedef {{ success: false, transient: true, error: string }} HunterTransient
 * @typedef {HunterSuccess | HunterTransient} HunterResult
 */

/**
 * Hunter domain-search with retry on network errors, 429, and 5xx
 * (3 retries with 2s/4s/8s backoff). Fatal errors (401/402/403) throw with a
 * `fatal: true` marker so the batch can halt the same way it does for
 * Apollo/Prospeo.
 *
 * On a transient failure that exhausts retries, returns
 * `{ success: false, transient: true }` instead of throwing — this lets the
 * caller fall through to the Prospeo/Apollo/scraper fallback chain rather
 * than failing the whole item on a temporary Hunter infrastructure blip.
 *
 * @param {string} domain
 * @returns {Promise<HunterResult>}
 */
export async function hunterDomainSearch(domain) {
  if (!process.env.HUNTER_API_KEY) {
    throw new Error('HUNTER_API_KEY not configured')
  }

  const url = `${DOMAIN_SEARCH_URL}?domain=${encodeURIComponent(domain)}&api_key=${process.env.HUNTER_API_KEY}`
  const maxRetries = 3
  let lastTransientError = 'Hunter request failed'

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    let response
    try {
      response = await fetch(url)
    } catch (err) {
      lastTransientError = `Network error: ${err instanceof Error ? err.message : String(err)}`
      if (attempt < maxRetries) {
        await sleep(backoffMs(attempt))
        continue
      }
      return { success: false, transient: true, error: lastTransientError }
    }

    if (response.status === 429 || response.status >= 500) {
      lastTransientError = `Hunter API ${response.status} ${response.statusText || ''}`.trim()
      if (attempt < maxRetries) {
        await sleep(backoffMs(attempt))
        continue
      }
      return { success: false, transient: true, error: lastTransientError }
    }

    let data
    try {
      data = await response.json()
    } catch {
      throw new Error(`Hunter API ${response.status} ${response.statusText} (non-JSON response)`)
    }

    if (response.status === 401 || response.status === 402 || response.status === 403) {
      /** @type {Error & { fatal?: boolean, code?: string }} */
      const err = new Error(`Hunter fatal: ${hunterApiErrorMessage(data, response.status)}`)
      err.fatal = true
      err.code =
        response.status === 401
          ? 'INVALID_API_KEY'
          : response.status === 402
            ? 'INSUFFICIENT_CREDITS'
            : 'FORBIDDEN'
      throw err
    }

    if (!response.ok) {
      throw new Error(hunterApiErrorMessage(data, response.status))
    }

    return { success: true, data }
  }

  return { success: false, transient: true, error: lastTransientError }
}
