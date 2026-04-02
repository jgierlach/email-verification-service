import Fastify from 'fastify'
import { verifyEmails } from './verify.js'

const PORT = parseInt(process.env.PORT || '3001', 10)
const API_TOKEN = process.env.API_TOKEN || ''
const LOG_LEVEL = process.env.LOG_LEVEL || 'info'
const MAX_BATCH_SIZE = 200

const fastify = Fastify({
  logger: {
    level: LOG_LEVEL,
  },
})

// Bearer token authentication hook
fastify.addHook('onRequest', async (request, reply) => {
  // Skip auth for health check
  if (request.url === '/health') return

  const authHeader = request.headers.authorization || ''
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : ''

  if (!API_TOKEN || token !== API_TOKEN) {
    reply.code(401).send({ error: 'Unauthorized' })
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

// Start server
const start = async () => {
  try {
    await fastify.listen({ port: PORT, host: '0.0.0.0' })
    fastify.log.info(`Email verification service listening on port ${PORT}`)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()
