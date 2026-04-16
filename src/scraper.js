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
 * Conservative list of public TLDs we accept. Domains whose last segment isn't
 * one of these (or doesn't start with one — see sanitizeEmail) are rejected.
 * Listed in intentional order: generic first, then country-code, then newer
 * gTLDs that matter for the current ICP. Sorted desc-by-length at use-time so
 * prefix-matching picks the longest valid TLD first.
 */
const KNOWN_TLDS = [
  // Generic
  'com', 'org', 'net', 'edu', 'gov', 'mil', 'int', 'info', 'biz', 'name', 'pro', 'mobi',
  // Country-code
  'us', 'uk', 'ca', 'au', 'nz', 'ie', 'de', 'fr', 'es', 'it', 'nl', 'jp', 'cn', 'br', 'mx', 'eu', 'in', 'ru', 'za',
  // Newer gTLDs common in small-biz / US ICP
  'co', 'io', 'ai', 'app', 'dev', 'tech', 'me', 'tv', 'club', 'site', 'online', 'shop',
  // Vertical gTLDs users actually register
  'church', 'ministry', 'charity', 'community', 'foundation', 'care', 'school',
  'health', 'center', 'academy', 'family', 'life', 'faith',
  // Longer gTLDs whose labels share a prefix with a generic (e.g. `edu` in
  // `education`, `com` in `communion`) — must be exact-listed; prefix-repair
  // must not fire on lowercase continuations.
  'communion', 'international', 'education',
]
const KNOWN_TLDS_BY_LENGTH = [...KNOWN_TLDS].sort((a, b) => b.length - a.length)

/**
 * After a known TLD prefix match on the last label, the remainder is only
 * treated as glued junk (safe to strip) if it does NOT look like a normal
 * DNS label continuation — e.g. `SubmitThanks` / `0ffice` / `-extra`, but
 * not `munion` from `.communion` or `cation` from `.education`.
 *
 * @param {string} remainder - `lastLabel.slice(tld.length)`
 */
function looksLikeTldGlueTail(remainder) {
  if (!remainder) return false
  const ch = remainder[0]
  return ch < 'a' || ch > 'z'
}

/**
 * Salvage an extracted email string. Handles the three scraper failure modes:
 *
 *   1. Multiple `@` signs (broken mailto like `sc_tn@brotherhood@gmail.com`).
 *   2. Trailing text concatenated to the TLD (`info@church.comSubmitThanks`
 *      → `info@church.com`; `.org0ffice` → `.org`). Lowercase-only tails are
 *      not stripped — avoids turning `@x.communion` into `@x.com`.
 *   3. Placeholder / test addresses (`example@...`, `test@...`).
 *
 * Returns the sanitized email (lowercased) or null if unsalvageable.
 *
 * @param {string} raw
 * @returns {string | null}
 */
function sanitizeEmail(raw) {
  if (!raw) return null
  const trimmed = raw.trim().replace(/^mailto:/i, '').split('?')[0]
  if (!trimmed) return null

  // Exactly one `@` — rejects `a@b@c` outright.
  const parts = trimmed.split('@')
  if (parts.length !== 2) return null
  const [local, rawDomain] = parts
  if (!local || !rawDomain) return null
  if (local.length > 64) return null
  if (!/^[A-Za-z0-9._%+\-]+$/.test(local)) return null

  // Placeholder local-parts seen in template/copy-paste text. Tail `\d*$`
  // catches numeric-suffixed variants like `example10`, `test3`.
  if (/^(example|sample|yourname|youremail|yourmail|firstname|lastname|first\.last|placeholder|lorem|test)\d*$/i.test(local)) {
    return null
  }

  const domain = rawDomain.toLowerCase()
  if (!domain.includes('.')) return null
  if (!/^[a-z0-9.\-]+$/.test(domain)) return null

  const segments = domain.split('.')
  const lastIdx = segments.length - 1
  const lastSeg = segments[lastIdx]

  // Fast path: last segment is an exact TLD match.
  if (KNOWN_TLDS.includes(lastSeg)) {
    return `${local.toLowerCase()}@${domain}`
  }

  // Repair path: last segment starts with a known TLD, then clearly glued junk
  // (digit / uppercase / punctuation). Do NOT strip when the rest is plain
  // lowercase — that is often a longer real TLD (`.communion`, `.education`).
  // `.comSubmitThanks` → `.com`, `.org0ffice` → `.org`.
  for (const tld of KNOWN_TLDS_BY_LENGTH) {
    if (!lastSeg.startsWith(tld)) continue
    const remainder = lastSeg.slice(tld.length)
    if (!looksLikeTldGlueTail(remainder)) continue
    const repaired = [...segments.slice(0, lastIdx), tld].join('.')
    return `${local.toLowerCase()}@${repaired}`
  }

  // Unknown TLD prefix — reject.
  return null
}

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
    const clean = sanitizeEmail(email)
    if (!clean) return
    if (byLower.has(clean)) return
    byLower.set(clean, { email: clean, method, pageUrl })
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
