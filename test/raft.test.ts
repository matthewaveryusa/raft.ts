import { TestMessagingEngine } from './test_messaging'
import { TestStorageEngine } from './test_storage'
import { TestTimeoutEngine } from './test_timeouts'

import { Server } from '../src/server'
import { State } from '../src/state'

import { AppendRequest, AppendResponse, Log, LogType, VoteRequest, VoteResponse } from '../src/messages'
import { EventLog } from './event_log'

function build_from_scratch() {
    console.log('build from scratch')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function build_from_state() {
    console.log('build from state')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()

    ev.logs.forEach((log) => {
        console.log(log)
    })

}

function make_invalid_state() {
    State.make('[1]')
}

function become_candidate() {
    console.log('become candidate')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()
    ev.logs[3].args.callback()
    ev.logs.forEach((log) => {
        console.log(log)
    })

}

function candidate_become_leader() {
    console.log('become leader')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()
    ev.logs[3].args.callback()
    ev.logs.forEach((log) => {
        console.log(log)
    })
    const vote_response_s2 = new VoteResponse(BigInt(42000002), 's2', 's1', BigInt(43), true)
    s.on_message(vote_response_s2)
    const vote_response_s3 = new VoteResponse(BigInt(42000003), 's3', 's1', BigInt(43), true)
    s.on_message(vote_response_s3)
}

function accept_vote_request() {
    console.log('accept vote request')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()

    // s2 sends a request with a higher term number
    const vote_request_s2 = new VoteRequest(BigInt(42000000), 's2', 's1', BigInt(43), BigInt(0), BigInt(0))
    s.on_message(vote_request_s2)

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function deny_vote_request() {
    console.log('deny vote request')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()

    // s2 sends a request with a higher term number
    const vote_request_s2 = new VoteRequest(BigInt(42000000), 's2', 's1', BigInt(41), BigInt(0), BigInt(0))
    s.on_message(vote_request_s2)

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function client_request() {
    console.log('client request')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()
    const ret_err = s.on_client_request(Buffer.from('test data'))
    s.promote_to_leader()

    const ret = s.on_client_request(Buffer.from('test data'))

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function append_request() {
    console.log('append request')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()

    // s2 sends a request with a higher term number (heartbeat)
    const append_request_s2 = new AppendRequest(BigInt(42000000), 's2', 's1', BigInt(43),
     BigInt(0), BigInt(0), BigInt(0), BigInt(0), [])
    s.on_message(append_request_s2)

    // s2 sends a request with a same term number (and data)
    const log = new Log()
    log.data = null
    log.idx = BigInt(1)
    log.term = BigInt(43)
    log.type = LogType.noop
    const append_request2_s2 = new AppendRequest(BigInt(42000001), 's2', 's1', BigInt(43),
     BigInt(0), BigInt(0), BigInt(0), BigInt(1), [log])
    s.on_message(append_request2_s2)

    // advance the commit index
    const append_request3_s2 = new AppendRequest(BigInt(42000002), 's2', 's1', BigInt(43),
     BigInt(1), BigInt(43), BigInt(1), BigInt(1), [])
    s.on_message(append_request3_s2)

    ev.logs.forEach((event) => {
        console.log(event)
    })
}

function append_response() {
    console.log('append response')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(5000)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()
    s.promote_to_leader()

    const ret = s.on_client_request(Buffer.from('test data'))
    const append_response_s2 = new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
    s.on_message(append_response_s2)

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

build_from_scratch()

build_from_state()

make_invalid_state()

become_candidate()

candidate_become_leader()

accept_vote_request()

deny_vote_request()

client_request()

append_request()

append_response()
