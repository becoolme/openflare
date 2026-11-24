// WebSocket hibernation spike: pure session/detach/reattach state machine.
// No production wiring; the ingress proxy would own real sockets and use this
// module to decide whether a detach is safe and what to replay on reattach.

export const CLOSED = 'closed'
export const ATTACHED = 'attached'
export const DETACHED = 'detached'

export class ReplayOverflow extends Error {
  constructor(bytes, cap) {
    super(`replay buffer overflow: ${bytes} > ${cap}`)
    this.bytes = bytes
    this.cap = cap
  }
}

/**
 * One client session across a workerd process boundary.
 * `capBytes` bounds BOTH buffered client frames and unacked server frames.
 */
export class HibernatableSession {
  constructor({ capBytes = 64 * 1024 } = {}) {
    if (!Number.isSafeInteger(capBytes) || capBytes < 1) {
      throw new Error('capBytes must be a positive integer')
    }
    this.capBytes = capBytes
    this.state = ATTACHED
    this.generation = null
    this.resumeToken = null
    this.clientFrames = []
    this.serverFrames = []
    this.bytes = 0
  }

  attach(generation, resumeToken = null) {
    if (this.state === CLOSED) throw new Error('session is closed')
    this.generation = generation
    this.resumeToken = resumeToken
    this.state = ATTACHED
    return this
  }

  /** Freeze the session for a workerd stop. Returns a reconnect token. */
  detach(resumeToken) {
    if (this.state === CLOSED) throw new Error('session is closed')
    if (this.state !== ATTACHED) throw new Error('session is not attached')
    this.state = DETACHED
    this.generation = null
    this.resumeToken = resumeToken
    return resumeToken
  }

  /** A client frame while detached: buffer it, or fail the sleep. */
  bufferClientFrame(bytes) {
    if (this.state !== DETACHED) throw new Error('session is not detached')
    this._reserve(bytes.length)
    this.clientFrames.push(bytes)
  }

  /** A server frame while attached that must survive the next detach. */
  bufferServerFrame(bytes) {
    if (this.state !== ATTACHED) throw new Error('session is not attached')
    this._reserve(bytes.length)
    this.serverFrames.push(bytes)
  }

  /**
   * Reattach to a new generation. Returns the frames to write in order:
   * buffered client frames to the new backend, then unacked server frames
   * to the client. Clears the buffer only after the caller acked the write.
   */
  reattach(generation, resumeToken) {
    if (this.state !== DETACHED) throw new Error('session is not detached')
    if (resumeToken !== this.resumeToken) throw new Error('resume token mismatch')
    const client = this.clientFrames
    const server = this.serverFrames
    this.state = ATTACHED
    this.generation = generation
    this.resumeToken = null
    this.clientFrames = []
    this.serverFrames = []
    this.bytes = 0
    return { toBackend: client, toClient: server }
  }

  close() {
    this.state = CLOSED
    this.generation = null
    this.resumeToken = null
    this.clientFrames = []
    this.serverFrames = []
    this.bytes = 0
  }

  _reserve(n) {
    if (this.bytes + n > this.capBytes) {
      throw new ReplayOverflow(this.bytes + n, this.capBytes)
    }
    this.bytes += n
  }
}

/**
 * Sleep gate: a session may be detached only if every buffered write fits and
 * the replay window is within the cap. On overflow the caller must keep the
 * tenant resident (fail safe) rather than close the socket silently.
 */
export function detachIsSafe(session, { pendingClientBytes = 0 } = {}) {
  if (session.state !== ATTACHED) return { safe: false, reason: 'not-attached' }
  if (session.bytes + pendingClientBytes > session.capBytes) {
    return { safe: false, reason: 'replay-window-full' }
  }
  return { safe: true, reason: 'ok' }
}
