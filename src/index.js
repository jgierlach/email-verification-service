import Fastify from 'fastify'
import secureJsonParse from 'secure-json-parse'
import { verifyEmails } from './verify.js'
import {
  runBatch,
  cancelBatch,
  getBatchStatus,
  getBatchMetrics,
  getProcessCacheSnapshot,
  recoverOnStartup,
} from './batch-runner.js'
import { supabaseEnabled } from './supabase.js'

const PORT = parseInt(process.env.PORT || '3001', 10)
const API_TOKEN = process.env.API_TOKEN || ''
const LOG_LEVEL = process.env.LOG_LEVEL || 'info'
const MAX_BATCH_SIZE = 200

const fastify = Fastify({
  logger: {
    level: LOG_LEVEL,
  },
})

// Fastify's default JSON parser throws FST_ERR_CTP_EMPTY_JSON_BODY when a
// request advertises Content-Type: application/json but sends an empty body.
// Some of our endpoints (POST /batches/:id/run, /cancel) take no body at all —
// the ID in the URL is sufficient. Register a tolerant parser that treats an
// empty/whitespace payload as an empty object. Use secure-json-parse (same
// family Fastify uses by default) so prototype poisoning via `__proto__` /
// `constructor` keys cannot slip through this custom path.
fastify.addContentTypeParser('application/json', { parseAs: 'string' }, (_req, body, done) => {
  try {
    const trimmed = typeof body === 'string' ? body.trim() : ''
    if (trimmed === '') return done(null, {})
    done(null, secureJsonParse(trimmed))
  } catch (err) {
    err.statusCode = 400
    done(err, undefined)
  }
})

// Bearer token authentication hook
fastify.addHook('onRequest', async (request, reply) => {
  // Skip auth for health check
  if (request.url === '/health') return

  const authHeader = request.headers.authorization || ''
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : ''

  if (!API_TOKEN || token !== API_TOKEN) {
    return reply.code(401).send({ error: 'Unauthorized' })
  }
})

// Health check
fastify.get('/health', async () => {
  return { status: 'ok' }
})

// Email verification endpoint
fastify.post('/verify', async (request, reply) => {
  const { emails } = /** @type {{ emails?: string[] }} */ (request.body) || {}

  if (!Array.isArray(emails)) {
    return reply.code(400).send({ error: 'emails must be an array' })
  }

  if (emails.length === 0) {
    return reply.code(400).send({ error: 'emails array is empty' })
  }

  if (emails.length > MAX_BATCH_SIZE) {
    return reply
      .code(400)
      .send({ error: `Batch size exceeds maximum of ${MAX_BATCH_SIZE}. Received ${emails.length}.` })
  }

  // Validate all items are strings
  if (!emails.every((e) => typeof e === 'string')) {
    return reply.code(400).send({ error: 'All items in emails array must be strings' })
  }

  fastify.log.info(`Verifying ${emails.length} email(s)`)

  const results = await verifyEmails(emails, { logger: fastify.log })

  return { results }
})

/**
 * POST /batches/:id/run
 * Kick off a long-running batch. Returns 202 immediately; the VPS owns the
 * batch lifecycle from this point. The batch ID must already exist in
 * Supabase `validation_batches` with pending items populated.
 */
fastify.post('/batches/:id/run', async (request, reply) => {
  if (!supabaseEnabled) {
    return reply.code(500).send({ error: 'Batch runner not configured (missing SUPABASE env vars)' })
  }

  const { id } = /** @type {{ id: string }} */ (request.params)
  if (!id) {
    return reply.code(400).send({ error: 'Batch id is required' })
  }

  // Fire-and-forget — we don't await the batch. Errors get logged by runBatch itself.
  runBatch(id, fastify.log).catch((err) => {
    fastify.log.error(
      { batchId: id, err: err instanceof Error ? err.message : String(err) },
      'Batch runner threw synchronously',
    )
  })

  return reply.code(202).send({ batch_id: id, status: 'accepted' })
})

/**
 * POST /batches/:id/cancel
 * Stop a running batch. Marks the batch `cancelled` in Supabase; the in-process
 * loop checks this between chunks and exits cleanly.
 */
fastify.post('/batches/:id/cancel', async (request, reply) => {
  if (!supabaseEnabled) {
    return reply.code(500).send({ error: 'Batch runner not configured' })
  }
  const { id } = /** @type {{ id: string }} */ (request.params)
  await cancelBatch(id, fastify.log)
  return { batch_id: id, status: 'cancelled' }
})

/**
 * GET /batches/:id/status
 * Read-only. Combines Supabase state with in-process presence.
 */
fastify.get('/batches/:id/status', async (request, reply) => {
  const { id } = /** @type {{ id: string }} */ (request.params)
  const result = await getBatchStatus(id)
  if (result.error) return reply.code(404).send(result)
  return result
})

/**
 * GET /batches/:id/metrics
 * Live per-batch throughput + cache metrics. Returns `{ in_process: false }`
 * when the batch isn't currently running in this worker — the final summary
 * is emitted to the service logs at batch completion.
 */
fastify.get('/batches/:id/metrics', async (request) => {
  const { id } = /** @type {{ id: string }} */ (request.params)
  return getBatchMetrics(id)
})

/**
 * GET /cache/catch-all
 * Ops endpoint — snapshot of the process-level catch-all cache.
 */
fastify.get('/cache/catch-all', async () => {
  return getProcessCacheSnapshot()
})

// Start server
const start = async () => {
  try {
    await fastify.listen({ port: PORT, host: '0.0.0.0' })
    fastify.log.info(`Email verification service listening on port ${PORT}`)

    // Reclaim stale items and auto-resume any in-flight batches after a restart.
    recoverOnStartup(fastify.log).catch((err) => {
      fastify.log.error({ err: err instanceof Error ? err.message : String(err) }, 'Startup recovery threw')
    })
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()
