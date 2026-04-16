import dns from 'node:dns/promises'
import crypto from 'node:crypto'
import { smtpVerify } from './smtp.js'
import { disposableDomains } from './disposable-domains.js'

const EMAIL_REGEX = /^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/

/**
 * @typedef {object} VerificationResult
 * @property {string} email
 * @property {'valid'|'invalid'|'catch-all'|'unknown'|'risky'} status
 * @property {number|null} smtp_response_code
 * @property {string|null} mx_host
 * @property {boolean} is_catch_all
 * @property {boolean} is_disposable
 */

/**
 * Simple semaphore for concurrency limiting.
 */
class Semaphore {
  /** @param {number} max */
  constructor(max) {
    this.max = max
    this.current = 0
    /** @type {Array<() => void>} */
    this.queue = []
  }

  acquire() {
    return new Promise((resolve) => {
      if (this.current < this.max) {
        this.current++
        resolve(undefined)
      } else {
        this.queue.push(resolve)
      }
    })
  }

  release() {
    this.current--
    if (this.queue.length > 0) {
      this.current++
      const next = this.queue.shift()
      next()
    }
  }
}

/**
 * Shared-infrastructure buckets — multiple per-tenant MX hostnames that all
 * route to the same provider's anti-abuse infrastructure get rolled up to a
 * single bucket key. Without this, a batch hitting 200 different M365 tenants
 * would create 200 independent per-MX limiters, each at maxPerDomain=2, while
 * Microsoft's source-IP anti-abuse sees ~400 simultaneous connections from us.
 *
 * `key` is the synthetic bucket id used in DomainLimiter.domains.
 * `maxPerDomain` and `delayMs` override the limiter's defaults for that bucket
 * — Microsoft is the strictest receiver in the wild, so it gets aggressive
 * throttling regardless of `MAX_PER_DOMAIN` / `DOMAIN_DELAY_MS` env settings.
 *
 * Math: 30s delay × 1 concurrent ≈ 120 RCPT/hour to Microsoft from this IP,
 * comfortably under their ~50/hr soft-block threshold per IP.
 *
 * @type {Array<{ match: RegExp, key: string, maxPerDomain?: number, delayMs?: number }>}
 */
const SHARED_INFRA_BUCKETS = [
  // Microsoft Office 365 / Exchange Online — strictest receiver. All M365
  // tenants funnel through *.mail.protection.outlook.com or *.olc.protection.outlook.com.
  { match: /(?:^|\.)(?:mail|olc)\.protection\.outlook\.com$/i, key: '__microsoft__', maxPerDomain: 1, delayMs: 30000 },
  // Google Workspace + consumer Gmail. *.aspmx.l.google.com is the canonical
  // Workspace MX; google.com / googlemail.com cover variants.
  { match: /(?:^|\.)(?:aspmx\.l\.google|google|googlemail)\.com$/i, key: '__google__', maxPerDomain: 2, delayMs: 8000 },
  // Yahoo + AOL.
  { match: /(?:^|\.)yahoodns\.net$/i, key: '__yahoo__', maxPerDomain: 1, delayMs: 15000 },
]

/**
 * Resolve an MX hostname to a (bucket-key, optional overrides) tuple. Unknown
 * MXes pass through with the lowercased host as their bucket key — preserving
 * existing per-MX behaviour for everything we don't explicitly group.
 *
 * @param {string} mxHost
 */
function resolveBucket(mxHost) {
  if (!mxHost) return { key: '', overrideMax: null, overrideDelay: null }
  const lower = mxHost.toLowerCase()
  for (const b of SHARED_INFRA_BUCKETS) {
    if (b.match.test(lower)) {
      return {
        key: b.key,
        overrideMax: b.maxPerDomain ?? null,
        overrideDelay: b.delayMs ?? null,
      }
    }
  }
  return { key: lower, overrideMax: null, overrideDelay: null }
}

/**
 * Per-MX rate limiter with shared-infrastructure rollups. Caps concurrent
 * connections and enforces a minimum delay between probes to the same bucket.
 * Bucket = either the MX hostname itself or a synthetic key for shared
 * infrastructure (Microsoft, Google, Yahoo) — see SHARED_INFRA_BUCKETS.
 */
class DomainLimiter {
  /**
   * @param {number} maxPerDomain
   * @param {number} delayMs - Nominal delay between connections to the same MX.
   * @param {number} jitterPct - Random ±% variation applied to `delayMs`. Defeats
   *   timing-pattern fingerprinting that rate-limit systems sometimes watch for.
   */
  constructor(maxPerDomain = 2, delayMs = 4000, jitterPct = 0.25) {
    this.maxPerDomain = maxPerDomain
    this.delayMs = delayMs
    this.jitterPct = jitterPct
    /**
     * @type {Map<string, { active: number, lastConnection: number, queue: Array<() => void> }>}
     * Keyed by bucket id (MX host or shared-infra synthetic key).
     * `active` counts callers past the concurrency gate, including any awaiting the inter-connect delay.
     */
    this.domains = new Map()
  }

  /** Randomized delay within ±jitterPct of the nominal. */
  _jitteredDelay(baseDelay = this.delayMs) {
    if (this.jitterPct <= 0) return baseDelay
    const factor = 1 + (Math.random() * 2 - 1) * this.jitterPct
    return Math.max(0, Math.floor(baseDelay * factor))
  }

  /** @param {string} mxHost */
  async acquire(mxHost) {
    const { key, overrideMax, overrideDelay } = resolveBucket(mxHost)
    const maxPerDomain = overrideMax ?? this.maxPerDomain
    const baseDelay = overrideDelay ?? this.delayMs

    if (!this.domains.has(key)) {
      this.domains.set(key, { active: 0, lastConnection: 0, queue: [] })
    }

    const state = this.domains.get(key)

    // Wait if at max concurrent "slots" for this bucket (reserved before delay — see below).
    while (state.active >= maxPerDomain) {
      await new Promise((resolve) => {
        state.queue.push(resolve)
      })
    }

    // Reserve a slot before any further `await`. Otherwise many tasks could pass the check
    // while `active` is still low, all sleep on the delay in parallel, then each increment and
    // blow past `maxPerDomain`.
    state.active++

    try {
      // Enforce delay between connections to the same bucket.
      const delay = this._jitteredDelay(baseDelay)
      const now = Date.now()
      const elapsed = now - state.lastConnection
      if (elapsed < delay && state.lastConnection > 0) {
        await new Promise((resolve) => setTimeout(resolve, delay - elapsed))
      }
      state.lastConnection = Date.now()
    } catch (err) {
      state.active--
      if (state.queue.length > 0) {
        const next = state.queue.shift()
        next()
      }
      throw err
    }
  }

  /** @param {string} mxHost */
  release(mxHost) {
    const { key } = resolveBucket(mxHost)
    const state = this.domains.get(key)
    if (!state) return

    state.active--
    if (state.queue.length > 0) {
      const next = state.queue.shift()
      next()
    }
  }
}

/**
 * Verify a single email through the full pipeline.
 * @param {string} email
 * @param {object} options
 * @param {string} options.ehloDomain
 * @param {string} options.mailFrom
 * @param {Semaphore} options.semaphore
 * @param {DomainLimiter} options.domainLimiter
 * @param {typeof console} options.logger
 * @param {Map<string, boolean>} [options.catchAllDomainCache] - Shared across the batch. Values: true=catch-all, false=not.
 * @returns {Promise<VerificationResult>}
 */
async function verifySingle(email, { ehloDomain, mailFrom, semaphore, domainLimiter, logger, catchAllDomainCache }) {
  const baseResult = {
    email,
    status: /** @type {const} */ ('unknown'),
    smtp_response_code: null,
    mx_host: null,
    is_catch_all: false,
    is_disposable: false,
  }

  // Step 1: Syntax check
  if (!EMAIL_REGEX.test(email)) {
    logger.info(`[${email}] Invalid syntax`)
    return { ...baseResult, status: 'invalid' }
  }

  const domain = email.split('@')[1].toLowerCase()

  // Step 2: Disposable domain check
  if (disposableDomains.has(domain)) {
    logger.info(`[${email}] Disposable domain: ${domain}`)
    return { ...baseResult, status: 'risky', is_disposable: true }
  }

  // Step 3: MX lookup
  /** @type {dns.MxRecord[]} */
  let mxRecords
  try {
    mxRecords = await dns.resolveMx(domain)
  } catch (err) {
    logger.info(
      `[${email}] No MX records for ${domain}: ${err instanceof Error ? err.message : String(err)}`,
    )
    return { ...baseResult, status: 'invalid' }
  }

  if (!mxRecords || mxRecords.length === 0) {
    logger.info(`[${email}] No MX records for ${domain}`)
    return { ...baseResult, status: 'invalid' }
  }

  // Sort by priority (lowest number = highest priority)
  mxRecords.sort((a, b) => a.priority - b.priority)
  const mxHost = mxRecords[0].exchange

  // Step 4: SMTP handshake (with concurrency control)
  await semaphore.acquire()
  await domainLimiter.acquire(mxHost)

  let smtpResult
  try {
    logger.info(`[${email}] SMTP check against ${mxHost}`)
    smtpResult = await smtpVerify({ email, mxHost, ehloDomain, mailFrom })
  } finally {
    domainLimiter.release(mxHost)
    semaphore.release()
  }

  const { responseCode } = smtpResult
  baseResult.mx_host = mxHost
  baseResult.smtp_response_code = responseCode

  // Classify SMTP response
  if (responseCode === null) {
    logger.info(`[${email}] SMTP timeout/error: ${smtpResult.rawResponse}`)
    return { ...baseResult, status: 'unknown' }
  }

  if ([550, 551, 552, 553].includes(responseCode)) {
    logger.info(`[${email}] Invalid (${responseCode})`)
    return { ...baseResult, status: 'invalid' }
  }

  // 4xx soft rejects often indicate throttling / greylisting / IP reputation
  // issues. We log at `warn` level so they stand out in pm2 output — a spike
  // here is the earliest signal that we need to slow down.
  if ([421, 450, 451, 452].includes(responseCode)) {
    logger.warn(`[${email}] SMTP soft reject (${responseCode}) from ${mxHost}: ${smtpResult.rawResponse}`)
    return { ...baseResult, status: 'unknown' }
  }

  if (responseCode !== 250) {
    logger.info(`[${email}] Unexpected response code: ${responseCode}`)
    return { ...baseResult, status: 'unknown' }
  }

  // Step 5: Catch-all detection (cached per-batch when possible).
  if (catchAllDomainCache?.has(domain)) {
    const isCatchAll = catchAllDomainCache.get(domain)
    if (isCatchAll) {
      logger.info(`[${email}] Catch-all domain (cached)`)
      return { ...baseResult, status: 'catch-all', is_catch_all: true }
    }
    logger.info(`[${email}] Valid (catch-all cache miss)`)
    return { ...baseResult, status: 'valid' }
  }

  const gibberish = crypto.randomBytes(8).toString('hex') + '@' + domain
  logger.info(`[${email}] Catch-all check with ${gibberish}`)

  await semaphore.acquire()
  await domainLimiter.acquire(mxHost)

  let catchAllResult
  try {
    catchAllResult = await smtpVerify({ email: gibberish, mxHost, ehloDomain, mailFrom })
  } finally {
    domainLimiter.release(mxHost)
    semaphore.release()
  }

  const isCatchAll = catchAllResult.responseCode === 250
  if (catchAllDomainCache) catchAllDomainCache.set(domain, isCatchAll)

  if (isCatchAll) {
    logger.info(`[${email}] Catch-all domain detected`)
    return { ...baseResult, status: 'catch-all', is_catch_all: true }
  }

  logger.info(`[${email}] Valid`)
  return { ...baseResult, status: 'valid' }
}

/**
 * Verify a batch of emails with concurrency control.
 *
 * Throttling defaults are tuned for IP-reputation safety over raw speed.
 * Every knob is overridable via env var so you can tune in production
 * without redeploying code:
 *
 *   MAX_CONCURRENCY          — global simultaneous SMTP probes (default 5)
 *   MAX_PER_DOMAIN           — simultaneous probes per MX bucket (default 2)
 *   DOMAIN_DELAY_MS          — nominal delay between probes to same bucket (default 4000)
 *   DOMAIN_DELAY_JITTER_PCT  — ±% randomization on that delay (default 0.25)
 *
 * Shared-infrastructure receivers (Microsoft 365, Google Workspace, Yahoo)
 * are bucketed at the provider level — see SHARED_INFRA_BUCKETS above.
 * Microsoft, in particular, gets aggressive overrides (1 concurrent / 30s
 * delay) because their anti-abuse looks at source IP, not per-tenant. These
 * overrides ignore the env knobs above on purpose.
 *
 * @param {string[]} emails
 * @param {object} [options]
 * @param {string} [options.ehloDomain]
 * @param {string} [options.mailFrom]
 * @param {number} [options.maxConcurrency]
 * @param {number} [options.maxPerDomain]
 * @param {number} [options.domainDelayMs]
 * @param {number} [options.domainDelayJitterPct]
 * @param {typeof console} [options.logger]
 * @param {Map<string, boolean>} [options.catchAllDomainCache] - Shared across calls within one batch for per-domain catch-all memoization.
 * @param {(result: VerificationResult) => void | Promise<void>} [options.onResult] -
 *   Fires as each email completes (in whatever order they finish, not input order).
 *   Lets callers commit per-email progress instead of waiting for the whole batch.
 * @returns {Promise<VerificationResult[]>}
 */
export async function verifyEmails(emails, options = {}) {
  const {
    ehloDomain = process.env.EHLO_DOMAIN || 'mx-verify.com',
    mailFrom = process.env.MAIL_FROM_ADDRESS || 'verify@mx-verify.com',
    maxConcurrency = parseInt(process.env.MAX_CONCURRENCY || '5', 10),
    maxPerDomain = parseInt(process.env.MAX_PER_DOMAIN || '2', 10),
    domainDelayMs = parseInt(process.env.DOMAIN_DELAY_MS || '4000', 10),
    domainDelayJitterPct = parseFloat(process.env.DOMAIN_DELAY_JITTER_PCT || '0.25'),
    logger = console,
    catchAllDomainCache,
    onResult,
  } = options

  const semaphore = new Semaphore(maxConcurrency)
  const domainLimiter = new DomainLimiter(maxPerDomain, domainDelayMs, domainDelayJitterPct)

  const results = await Promise.all(
    emails.map(async (email) => {
      const result = await verifySingle(email.trim().toLowerCase(), {
        ehloDomain,
        mailFrom,
        semaphore,
        domainLimiter,
        logger,
        catchAllDomainCache,
      })
      if (onResult) {
        try {
          await onResult(result)
        } catch (err) {
          logger.error(
            { email: result.email, err: err instanceof Error ? err.message : String(err) },
            'onResult callback threw — swallowing so one failure does not tank the batch',
          )
        }
      }
      return result
    })
  )

  return results
}
