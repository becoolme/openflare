import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  ATTACHED,
  DETACHED,
  HibernatableSession,
  ReplayOverflow,
  detachIsSafe,
} from './state.mjs'

test('detach buffers client frames and replays them in order', () => {
  const s = new HibernatableSession({ capBytes: 1024 }).attach('gen-1')
  s.bufferServerFrame(Buffer.from('hello'))
  s.detach('token-1')
  assert.equal(s.state, DETACHED)
  s.bufferClientFrame(Buffer.from('a'))
  s.bufferClientFrame(Buffer.from('b'))
  const replay = s.reattach('gen-2', 'token-1')
  assert.deepEqual(replay.toBackend, [Buffer.from('a'), Buffer.from('b')])
  assert.deepEqual(replay.toClient, [Buffer.from('hello')])
  assert.equal(s.state, ATTACHED)
})

test('bounded buffer refuses an unsafe sleep instead of dropping frames', () => {
  const s = new HibernatableSession({ capBytes: 8 }).attach('gen-1')
  s.detach('t')
  s.bufferClientFrame(Buffer.from('1234'))
  assert.throws(() => s.bufferClientFrame(Buffer.from('56789')), ReplayOverflow)
  // The overflow keeps the session detached but the caller sees the error and
  // must keep the tenant resident; no frame was lost.
  assert.equal(s.bytes, 4)
})

test('detach gate accounts for pending client bytes', () => {
  const s = new HibernatableSession({ capBytes: 10 }).attach('gen-1')
  assert.deepEqual(detachIsSafe(s, { pendingClientBytes: 4 }), { safe: true, reason: 'ok' })
  assert.deepEqual(detachIsSafe(s, { pendingClientBytes: 11 }), {
    safe: false,
    reason: 'replay-window-full',
  })
})

test('resume token is required and single-use', () => {
  const s = new HibernatableSession().attach('gen-1')
  s.detach('token')
  assert.throws(() => s.reattach('gen-2', 'wrong'), /resume token mismatch/)
  s.reattach('gen-2', 'token')
  assert.throws(() => s.reattach('gen-3', 'token'), /not detached/)
})

test('closed sessions never detach or reattach', () => {
  const s = new HibernatableSession().attach('gen-1')
  s.close()
  assert.throws(() => s.detach('t'), /closed/)
})
