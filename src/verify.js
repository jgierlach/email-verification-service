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
 * Per-domain rate limiter.
 * Enforces max concurrent connections and minimum delay between connections to the same MX.
 */
class DomainLimiter {
  /**
   * @param {number} maxPerDomain
   * @param {number} delayMs
   */
  constructor(maxPerDomain = 3, delayMs = 2000) {
    this.maxPerDomain = maxPerDomain
    this.delayMs = delayMs
    /** @type {Map<string, { active: number, lastConnection: number, queue: Array<() => void> }>} */
    this.domains = new Map()
  }

  /** @param {string} domain */
  async acquire(domain) {
    if (!this.domains.has(domain)) {
      this.domains.set(domain, { active: 0, lastConnection: 0, queue: [] })
    }

    const state = this.domains.get(domain)

    // Wait if at max concurrent connections for this domain
    if (state.active >= this.maxPerDomain) {
      await new Promise((resolve) => {
        state.queue.push(resolve)
      })
    }

    // Enforce delay between connections to the same MX
    const now = Date.now()
    const elapsed = now - state.lastConnection
    if (elapsed < this.delayMs && state.lastConnection > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.delayMs - elapsed))
    }

    state.active++
    state.lastConnection = Date.now()
  }

  /** @param {string} domain */
  release(domain) {
    const state = this.domains.get(domain)
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

  if ([450, 451, 452].includes(responseCode)) {
    logger.info(`[${email}] Temporarily unavailable (${responseCode})`)
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
 * @param {string[]} emails
 * @param {object} [options]
 * @param {string} [options.ehloDomain]
 * @param {string} [options.mailFrom]
 * @param {number} [options.maxConcurrency]
 * @param {typeof console} [options.logger]
 * @param {Map<string, boolean>} [options.catchAllDomainCache] - Shared across calls within one batch for per-domain catch-all memoization.
 * @returns {Promise<VerificationResult[]>}
 */
export async function verifyEmails(emails, options = {}) {
  const {
    ehloDomain = process.env.EHLO_DOMAIN || 'mx-verify.com',
    mailFrom = process.env.MAIL_FROM_ADDRESS || 'verify@mx-verify.com',
    maxConcurrency = parseInt(process.env.MAX_CONCURRENCY || '10', 10),
    logger = console,
    catchAllDomainCache,
  } = options

  const semaphore = new Semaphore(maxConcurrency)
  const domainLimiter = new DomainLimiter(3, 2000)

  const results = await Promise.all(
    emails.map((email) =>
      verifySingle(email.trim().toLowerCase(), {
        ehloDomain,
        mailFrom,
        semaphore,
        domainLimiter,
        logger,
        catchAllDomainCache,
      })
    )
  )

  return results
}
