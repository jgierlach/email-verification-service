import net from 'node:net'

/**
 * Send an SMTP command and wait for a response line.
 * Resolves with { code: number|null, message: string }.
 * @param {net.Socket} socket
 * @param {string|null} command - null means just wait for server greeting
 * @param {number} timeout
 * @returns {Promise<{ code: number|null, message: string }>}
 */
function smtpCommand(socket, command, timeout) {
  return new Promise((resolve, reject) => {
    let buffer = ''
    let settled = false

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true
        cleanup()
        resolve({ code: null, message: 'timeout' })
      }
    }, timeout)

    function onData(chunk) {
      buffer += chunk.toString()
      // SMTP multi-line responses have a dash after the code (e.g. "250-...")
      // The final line has a space (e.g. "250 OK")
      const lines = buffer.split('\r\n')
      for (const line of lines) {
        if (line.length >= 4 && line[3] === ' ') {
          const code = parseInt(line.substring(0, 3), 10)
          if (!settled) {
            settled = true
            cleanup()
            resolve({ code: isNaN(code) ? null : code, message: buffer.trim() })
          }
          return
        }
      }
    }

    function onError(err) {
      if (!settled) {
        settled = true
        cleanup()
        resolve({ code: null, message: err.message })
      }
    }

    function onClose() {
      if (!settled) {
        settled = true
        cleanup()
        resolve({ code: null, message: 'connection closed' })
      }
    }

    function cleanup() {
      clearTimeout(timer)
      socket.removeListener('data', onData)
      socket.removeListener('error', onError)
      socket.removeListener('close', onClose)
    }

    socket.on('data', onData)
    socket.on('error', onError)
    socket.on('close', onClose)

    if (command !== null) {
      socket.write(command + '\r\n')
    }
  })
}

/**
 * Perform an SMTP verification handshake against an MX host.
 * Opens a TCP connection on port 25, runs EHLO → MAIL FROM → RCPT TO → QUIT.
 * Never proceeds to DATA — no email is sent.
 *
 * @param {object} options
 * @param {string} options.email - The email address to verify
 * @param {string} options.mxHost - The MX server hostname
 * @param {string} [options.ehloDomain='mx-verify.com'] - Domain for EHLO
 * @param {string} [options.mailFrom='verify@mx-verify.com'] - MAIL FROM address
 * @param {number} [options.commandTimeout=7000] - Timeout per SMTP command (ms)
 * @param {number} [options.connectionTimeout=10000] - TCP connection timeout (ms)
 * @returns {Promise<{ responseCode: number|null, rawResponse: string }>}
 */
export async function smtpVerify({
  email,
  mxHost,
  ehloDomain = 'mx-verify.com',
  mailFrom = 'verify@mx-verify.com',
  commandTimeout = 7000,
  connectionTimeout = 10000,
}) {
  /** @type {net.Socket|null} */
  let socket = null

  try {
    // Open TCP connection
    socket = await new Promise((resolve, reject) => {
      const sock = net.createConnection({ host: mxHost, port: 25, timeout: connectionTimeout })

      const timer = setTimeout(() => {
        sock.destroy()
        reject(new Error('connection timeout'))
      }, connectionTimeout)

      sock.once('connect', () => {
        clearTimeout(timer)
        resolve(sock)
      })

      sock.once('error', (err) => {
        clearTimeout(timer)
        reject(err)
      })

      sock.once('timeout', () => {
        sock.destroy()
        clearTimeout(timer)
        reject(new Error('connection timeout'))
      })
    })

    // Wait for server greeting (220)
    const greeting = await smtpCommand(socket, null, commandTimeout)
    if (greeting.code !== 220) {
      return { responseCode: greeting.code, rawResponse: greeting.message }
    }

    // EHLO
    const ehlo = await smtpCommand(socket, `EHLO ${ehloDomain}`, commandTimeout)
    if (ehlo.code !== 250) {
      return { responseCode: ehlo.code, rawResponse: ehlo.message }
    }

    // MAIL FROM
    const mailFromResp = await smtpCommand(socket, `MAIL FROM:<${mailFrom}>`, commandTimeout)
    if (mailFromResp.code !== 250) {
      return { responseCode: mailFromResp.code, rawResponse: mailFromResp.message }
    }

    // RCPT TO — this is the actual verification
    const rcptTo = await smtpCommand(socket, `RCPT TO:<${email}>`, commandTimeout)

    // QUIT (best-effort, don't care about response)
    socket.write('QUIT\r\n')

    return { responseCode: rcptTo.code, rawResponse: rcptTo.message }
  } catch (err) {
    return { responseCode: null, rawResponse: err.message }
  } finally {
    if (socket) {
      socket.destroy()
    }
  }
}
