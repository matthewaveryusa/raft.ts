import { TestMessagingEngine } from './test_messaging'
import { TestStorageEngine } from './test_storage'
import { TestTimeoutEngine } from './test_timeouts'

import { Server } from '../src/server'
import { State } from '../src/state'

import { VoteResponse } from '../src/messages'
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
        console.log(log[0], log[1])
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
        console.log(log[0], log[1])
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
    ev.logs[3][1][2]()
    ev.logs.forEach((log) => {
        console.log(log[0], log[1])
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
    ev.logs[3][1][2]()
    ev.logs.forEach((log) => {
        console.log(log[0], log[1])
    })
    const vote_response_s2 = new VoteResponse(BigInt(42000002), 's2', 's1', BigInt(43), true)
    s.on_message(vote_response_s2)
    const vote_response_s3 = new VoteResponse(BigInt(42000003), 's3', 's1', BigInt(43), true)
    s.on_message(vote_response_s3)
}

build_from_scratch()
build_from_state()
make_invalid_state()
become_candidate()
candidate_become_leader()
