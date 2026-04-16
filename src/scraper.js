import * as cheerio from 'cheerio'

const FETCH_TIMEOUT_MS = 8000
const MAX_PAGES_PER_DOMAIN = 3
const USER_AGENT = 'CorelabsBot/1.0 (+contact email discovery)'

/**
 * URL-path scoring for contact-likely pages. Highest score wins; we fetch
 * the top 2 after the homepage. Patterns are deliberately conservative —
 * false positives cost us a fetch, not a credit.
 *
 * @type {Array<{ pattern: RegExp, score: number }>}
 */
const CONTACT_PATH_PATTERNS = [
  { pattern: /^\/contact\/?$/i, score: 100 },
  { pattern: /\/contact[-_]?us\/?$/i, score: 95 },
  { pattern: /\/contact\/?/i, score: 80 },
  { pattern: /\/staff\/?$/i, score: 75 },
  { pattern: /\/leadership\/?$/i, score: 75 },
  { pattern: /\/our[-_]?team\/?$/i, score: 72 },
  { pattern: /\/team\/?$/i, score: 70 },
  { pattern: /\/our[-_]?people\/?$/i, score: 70 },
  { pattern: /\/about[-_]?us\/?$/i, score: 65 },
  { pattern: /\/about\/?$/i, score: 60 },
  { pattern: /\/who[-_]?we[-_]?are\/?$/i, score: 55 },
  { pattern: /\/meet[-_]?the[-_]?team\/?$/i, score: 68 },
]

/** Localpart prefixes we reject outright (auto-responders, compliance, etc.) */
const DENY_PREFIX = [
  'no-reply@', 'noreply@', 'no.reply@', 'do-not-reply@', 'donotreply@',
  'mailer-daemon@', 'postmaster@', 'bounces@',
  'webmaster@',
  'privacy@', 'abuse@', 'dmca@', 'legal@', 'copyright@',
]

/** Domains that represent the hosting/platform, not the business. */
const DENY_DOMAINS = [
  'wix.com', 'wixsite.com', 'wixpress.com', // wixpress covers sentry.wixpress.com + sentry-next.wixpress.com DSN leaks
  'squarespace.com', 'godaddy.com', 'godaddysites.com',
  'weebly.com', 'jimdo.com', 'jimdofree.com', 'myshopify.com',
  'sentry.io', 'sentry-cdn.com', 'hubspot.com', 'wordpress.com', 'cloudflare.com',
  'example.com', 'test.com', 'domain.com', 'yourdomain.com', 'email.com',
]

const EMAIL_REGEX = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi

/**
 * Replace common in-text obfuscation tokens with their punctuation equivalent
 * before running the email regex. Covers the forms we actually see in small-
 * business websites: `[at]`, `(at)`, ` AT `, `[dot]`, etc.
 * @param {string} text
 */
function deobfuscate(text) {
  return text
    .replace(/\s*\[\s*at\s*\]\s*/gi, '@')
    .replace(/\s*\(\s*at\s*\)\s*/gi, '@')
    .replace(/\s+at\s+/gi, '@')
    .replace(/\s*\[\s*dot\s*\]\s*/gi, '.')
    .replace(/\s*\(\s*dot\s*\)\s*/gi, '.')
    .replace(/\s+dot\s+/gi, '.')
}

/**
 * @param {string} email
 * @returns {boolean} true if rejected (denylisted or malformed)
 */
function isDenylisted(email) {
  const lower = email.toLowerCase()
  if (!lower.includes('@') || !lower.includes('.')) return true

  for (const p of DENY_PREFIX) {
    if (lower.startsWith(p)) return true
  }

  const at = lower.lastIndexOf('@')
  const local = lower.slice(0, at)
  const emailDomain = lower.slice(at + 1)

  for (const d of DENY_DOMAINS) {
    if (emailDomain === d || emailDomain.endsWith(`.${d}`)) return true
  }

  // Reject addresses that look like filename/image refs.
  if (/\.(png|jpg|jpeg|gif|svg|webp)$/.test(lower)) return true

  // Consecutive dots are invalid per RFC 5321 and are a strong signal of
  // tracking-token junk (e.g. Sentry DSN leaks). Real emails don't have `..`.
  if (local.includes('..')) return true

  // Long opaque local-parts with no separators (no `.`, `-`, `_`) are almost
  // always telemetry tokens, not human addresses. Classic Sentry public-key
  // DSNs look like `a1b2c3d4e5f6...@host` with 32+ hex chars.
  if (local.length >= 32 && !/[._-]/.test(local)) return true

  return false
}

/**
 * Cheap same-domain check. `domain` is the website we scraped, `email` is
 * the address we found. Both sides normalized to their registrable-ish root
 * (strip `www.` and single-level subdomains).
 *
 * @param {string} email
 * @param {string} domain
 */
function isOnDomain(email, domain) {
  const at = email.lastIndexOf('@')
  if (at < 0) return false
  const emailDomain = email.slice(at + 1).toLowerCase().replace(/^www\./, '')
  const siteDomain = domain.toLowerCase().replace(/^www\./, '')

  if (emailDomain === siteDomain) return true
  if (emailDomain.endsWith(`.${siteDomain}`)) return true
  if (siteDomain.endsWith(`.${emailDomain}`)) return true
  return false
}

/**
 * Score a candidate link. Only same-host links get scored; external links
 * and non-HTTP schemes return 0.
 *
 * @param {string} href
 * @param {URL} baseUrl
 * @returns {{ score: number, absoluteUrl: string | null }}
 */
function scoreLink(href, baseUrl) {
  if (!href) return { score: 0, absoluteUrl: null }
  /** @type {URL} */
  let abs
  try {
    abs = new URL(href, baseUrl)
  } catch {
    return { score: 0, absoluteUrl: null }
  }
  if (abs.protocol !== 'http:' && abs.protocol !== 'https:') {
    return { score: 0, absoluteUrl: null }
  }
  // Only same-host links (ignore subdomain drift — keeps the scope tight).
  if (abs.hostname.replace(/^www\./, '') !== baseUrl.hostname.replace(/^www\./, '')) {
    return { score: 0, absoluteUrl: null }
  }
  const path = abs.pathname || '/'
  let best = 0
  for (const { pattern, score } of CONTACT_PATH_PATTERNS) {
    if (pattern.test(path)) best = Math.max(best, score)
  }
  return { score: best, absoluteUrl: abs.toString() }
}

/**
 * Fetch a URL with a short timeout, HTML-only. Returns null on any failure —
 * the scraper is best-effort and never throws.
 *
 * @param {string} url
 * @param {import('fastify').FastifyBaseLogger | typeof console} logger
 */
async function fetchHtml(url, logger) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)
  try {
    const res = await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'User-Agent': USER_AGENT,
        Accept: 'text/html,application/xhtml+xml',
      },
    })
    if (!res.ok) {
      logger.debug({ url, status: res.status }, '[scraper] fetch non-ok')
      return null
    }
    const contentType = res.headers.get('content-type') || ''
    if (!/text\/html|application\/xhtml/.test(contentType)) {
      logger.debug({ url, contentType }, '[scraper] fetch non-html')
      return null
    }
    return await res.text()
  } catch (err) {
    logger.debug({ url, err: err instanceof Error ? err.message : String(err) }, '[scraper] fetch error')
    return null
  } finally {
    clearTimeout(timeout)
  }
}

/**
 * Extract unique email addresses from an HTML document.
 *
 * @param {string} html
 * @param {string} pageUrl - absolute URL of the page (for source_metadata)
 * @returns {Array<{ email: string, method: 'mailto' | 'text_regex' | 'deobfuscated', pageUrl: string }>}
 */
function extractEmailsFromHtml(html, pageUrl) {
  const $ = cheerio.load(html)

  /** @type {Map<string, { email: string, method: 'mailto' | 'text_regex' | 'deobfuscated', pageUrl: string }>} */
  const byLower = new Map()

  /**
   * @param {string} email
   * @param {'mailto' | 'text_regex' | 'deobfuscated'} method
   */
  function add(email, method) {
    const clean = email.trim().replace(/^mailto:/i, '').split('?')[0]
    if (!clean) return
    const lower = clean.toLowerCase()
    if (byLower.has(lower)) return
    byLower.set(lower, { email: clean, method, pageUrl })
  }

  // 1) mailto: hrefs (highest confidence)
  $('a[href^="mailto:" i], a[href^="mailto%3A" i]').each((_, el) => {
    const href = $(el).attr('href') || ''
    const decoded = decodeURIComponent(href)
    add(decoded, 'mailto')
  })

  // 2) plain-text regex over visible body text
  const bodyText = $('body').text() || $.root().text() || ''
  const rawMatches = bodyText.match(EMAIL_REGEX) || []
  for (const m of rawMatches) add(m, 'text_regex')

  // 3) de-obfuscated pass over the same text (catches [at]/[dot] styles)
  const deobfuscated = deobfuscate(bodyText)
  if (deobfuscated !== bodyText) {
    const deobfMatches = deobfuscated.match(EMAIL_REGEX) || []
    for (const m of deobfMatches) {
      const lower = m.toLowerCase()
      if (!byLower.has(lower)) add(m, 'deobfuscated')
    }
  }

  return Array.from(byLower.values())
}

/**
 * Pick up to N additional pages to fetch beyond the homepage, ordered by
 * contact-likeliness score.
 *
 * @param {string} homepageHtml
 * @param {URL} baseUrl
 * @param {number} limit - max URLs to return; clamped to a non-negative integer
 *   (negative values would be interpreted by `Array#slice` as an end index).
 * @returns {string[]} absolute URLs
 */
function pickContactPageUrls(homepageHtml, baseUrl, limit) {
  const raw = Number(limit)
  const safeLimit = Number.isFinite(raw) ? Math.max(0, Math.floor(raw)) : 0

  const $ = cheerio.load(homepageHtml)
  /** @type {Map<string, number>} */
  const candidates = new Map()
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || ''
    const { score, absoluteUrl } = scoreLink(href, baseUrl)
    if (!absoluteUrl || score <= 0) return
    // keep highest score per absolute URL (page may be linked multiple times)
    const prev = candidates.get(absoluteUrl) || 0
    if (score > prev) candidates.set(absoluteUrl, score)
  })
  return Array.from(candidates.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, safeLimit)
    .map(([url]) => url)
}

/**
 * Build a sourced_contacts row from a scraped email. Caller fills in
 * website_id on insert.
 *
 * @param {{ email: string, method: 'mailto' | 'text_regex' | 'deobfuscated', pageUrl: string }} hit
 * @param {string} domain
 */
function buildContactRow(hit, domain) {
  return {
    first_name: null,
    last_name: null,
    name: null,
    title: null,
    email: hit.email,
    phone: null,
    source: 'scraper',
    source_metadata: {
      extraction_method: hit.method,
      page_url: hit.pageUrl,
      on_domain: isOnDomain(hit.email, domain),
    },
    verification_status: 'pending',
    is_decision_maker: false,
  }
}

/**
 * Main orchestrator. Best-effort — never throws for recoverable failures.
 * Returns a structured summary so the enrichment runner can log cleanly and
 * write an `enrichment_log` row regardless of outcome.
 *
 * @param {string} domain
 * @param {{ maxPages?: number }} [options]
 * @param {import('fastify').FastifyBaseLogger | typeof console} [logger]
 * @returns {Promise<{
 *   contacts: object[],
 *   pagesFetched: number,
 *   uniqueEmailsFound: number,
 *   elapsedMs: number,
 *   reason: 'ok' | 'no_contact_page' | 'fetch_failed' | 'timeout' | 'no_emails',
 * }>}
 */
export async function scrapeEmailsFromWebsite(domain, options = {}, logger = console) {
  const startedAt = Date.now()
  const maxPages = Math.min(options.maxPages ?? MAX_PAGES_PER_DOMAIN, 5)

  const homepageUrl = `https://${domain}`
  let baseUrl
  try {
    baseUrl = new URL(homepageUrl)
  } catch {
    return {
      contacts: [],
      pagesFetched: 0,
      uniqueEmailsFound: 0,
      elapsedMs: Date.now() - startedAt,
      reason: 'fetch_failed',
    }
  }

  const homepageHtml = await fetchHtml(homepageUrl, logger)
  if (!homepageHtml) {
    return {
      contacts: [],
      pagesFetched: 0,
      uniqueEmailsFound: 0,
      elapsedMs: Date.now() - startedAt,
      reason: 'fetch_failed',
    }
  }

  /** @type {Map<string, { email: string, method: 'mailto' | 'text_regex' | 'deobfuscated', pageUrl: string }>} */
  const accumulated = new Map()
  let pagesFetched = 1

  // Homepage always contributes.
  for (const hit of extractEmailsFromHtml(homepageHtml, homepageUrl)) {
    accumulated.set(hit.email.toLowerCase(), hit)
  }

  // Follow up to (maxPages - 1) contact-likely internal links.
  const nextUrls = pickContactPageUrls(homepageHtml, baseUrl, maxPages - 1)
  for (const nextUrl of nextUrls) {
    const html = await fetchHtml(nextUrl, logger)
    if (!html) continue
    pagesFetched++
    for (const hit of extractEmailsFromHtml(html, nextUrl)) {
      if (!accumulated.has(hit.email.toLowerCase())) {
        accumulated.set(hit.email.toLowerCase(), hit)
      }
    }
  }

  const uniqueEmailsFound = accumulated.size

  // Apply denylist, then build contact rows.
  /** @type {object[]} */
  const contacts = []
  for (const hit of accumulated.values()) {
    if (isDenylisted(hit.email)) continue
    contacts.push(buildContactRow(hit, domain))
  }

  const elapsedMs = Date.now() - startedAt

  let reason = 'ok'
  if (uniqueEmailsFound === 0) {
    reason = nextUrls.length === 0 ? 'no_contact_page' : 'no_emails'
  } else if (contacts.length === 0) {
    // Emails existed but all were denylisted
    reason = 'no_emails'
  }

  logger.info(
    {
      domain,
      pagesFetched,
      uniqueEmailsFound,
      accepted: contacts.length,
      elapsedMs,
      reason,
    },
    '[scraper] domain done',
  )

  return { contacts, pagesFetched, uniqueEmailsFound, elapsedMs, reason }
}
