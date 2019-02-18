import { TestMessagingEngine } from './test_messaging'
import { TestStorageEngine } from './test_storage'
import { TestTimeoutEngine } from './test_timeouts'

import { Server } from '../src/server'
import { State } from '../src/state'

import { AppendRequest, AppendResponse, Log, LogType, Message,
    MessageType, VoteRequest, VoteResponse } from '../src/messages'
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

    // no-op to evaluate the else branch
    s.save_state()

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
    const vote_response_s2 = new VoteResponse(BigInt(42000002), 's2', 's1', BigInt(43), true)
    s.on_message(vote_response_s2)
    const vote_response_s4 = new VoteResponse(BigInt(42000004), 's4', 's1', BigInt(43), false)
    s.on_message(vote_response_s4)
    const vote_response_s3 = new VoteResponse(BigInt(42000003), 's3', 's1', BigInt(43), true)
    s.on_message(vote_response_s3)

    // I'm a leader, can't start a vode. no-op
    s.candidate_start_vote()

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function candidate_step_down() {
    console.log('candidate step down')
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
    const vote_response_s2 = new VoteResponse(BigInt(42000002), 's2', 's1', BigInt(420), false)
    s.on_message(vote_response_s2)

    // I'm a leader, can't start a vode. no-op
    s.candidate_start_vote()

    ev.logs.forEach((log) => {
        console.log(log)
    })
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
    vote_request_s2.id++
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
    state.commit_idx = BigInt(0)
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

    s.on_client_request(Buffer.from('test data'))
    const append_response_s2 = new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
    s.on_message(append_response_s2)
    const append_response_s3 = new AppendResponse(BigInt(42000003), 's3', 's1', BigInt(42), false)
    s.on_message(append_response_s3)
    const append_response2_s3 = new AppendResponse(BigInt(42000005), 's3', 's1', BigInt(42), true)
    s.on_message(append_response2_s3)
    const append_response_s4 = new AppendResponse(BigInt(42000004), 's4', 's1', BigInt(42), false)
    s.on_message(append_response_s4)
    const append_response2_s4 = new AppendResponse(BigInt(42000006), 's4', 's1', BigInt(42), false)
    s.on_message(append_response2_s4)

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function trigger_heartbeat() {
    console.log('trigger heartbeat')
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

    ev.logs[9].args.callback()

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function append_to_trailing_peer() {
    console.log('append to trailing peer')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(0)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()
    s.promote_to_leader()

    let ret = s.on_client_request(Buffer.from('test data'))
    const append_response_s2 = new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
    s.on_message(append_response_s2)
    const append_response_s3 = new AppendResponse(BigInt(42000003), 's3', 's1', BigInt(42), false)
    s.on_message(append_response_s3)
    // add another log. s3 has optimistic appends turned off
    ret = s.on_client_request(Buffer.from('test data2'))
    const append_response2_s3 = new AppendResponse(BigInt(42000005), 's3', 's1', BigInt(42), true)
    // another append with 'test data2' should be sent to s3
    s.on_message(append_response2_s3)

    ev.logs.forEach((log) => {
        console.log(log)
    })
}

function find_common_log() {
    console.log('find common log')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    const l1 = new Log()
    l1.data = Buffer.from('1')
    l1.idx = BigInt(1)
    l1.term = BigInt(40)
    l1.type = LogType.message
    se.add_log_to_storage(l1)
    const l2 = new Log()
    l2.data = Buffer.from('2')
    l2.idx = BigInt(2)
    l2.term = BigInt(41)
    l2.type = LogType.message
    se.add_log_to_storage(l2)
    const l3 = new Log()
    l3.data = Buffer.from('3')
    l3.idx = BigInt(3)
    l3.term = BigInt(42)
    l3.type = LogType.message
    se.add_log_to_storage(l3)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(1)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()

    const l33 = new Log()
    l33.data = Buffer.from('33')
    l33.idx = BigInt(3)
    l33.term = BigInt(43)
    l33.type = LogType.message
    const l4 = new Log()
    l4.data = Buffer.from('44')
    l4.idx = BigInt(4)
    l4.term = BigInt(43)
    l4.type = LogType.message

    // bad term
    const append_request_s2_fail1 = new AppendRequest(BigInt(42000000), 's2', 's1', BigInt(12),
    BigInt(2), BigInt(12), BigInt(1), BigInt(4), [l33, l4])
    s.on_message(append_request_s2_fail1)

    // bad prev term
    const append_request_s2_fail2 = new AppendRequest(BigInt(42000001), 's2', 's1', BigInt(43),
    BigInt(2), BigInt(43), BigInt(1), BigInt(4), [l33, l4])
    s.on_message(append_request_s2_fail2)

    // bad prev index
    const append_request_s2_fail3 = new AppendRequest(BigInt(42000002), 's2', 's1', BigInt(43),
    BigInt(15), BigInt(42), BigInt(1), BigInt(4), [l33, l4])
    s.on_message(append_request_s2_fail3)

    // trying to clobber commited -- no bueno
    const append_request_s2_fail4 = new AppendRequest(BigInt(42000003), 's2', 's1', BigInt(43),
    BigInt(0), BigInt(0), BigInt(1), BigInt(4), [l33, l4])
    s.on_message(append_request_s2_fail4)

    // ok, delete bad logs
    const append_request_s2 = new AppendRequest(BigInt(42000004), 's2', 's1', BigInt(43),
     BigInt(2), BigInt(41), BigInt(0), BigInt(4), [l33, l4])
    s.on_message(append_request_s2)

    ev.logs.forEach((l) => {
        console.log(l)
    })
}

function various_invalid_messages() {
    console.log('various invalid messages')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(0)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me)
    s.start_server()
    // not a leader, no-op
    s.leader_append_entry(new Log())
    let msg: Message = new VoteRequest(BigInt(1), 'bad', 's1', BigInt(0), BigInt(0), BigInt(0))
    s.on_message(msg)
    msg.to = 'bad'
    msg.from = 's2'
    s.on_message(msg)

    msg = new VoteRequest(BigInt(1000), 's2', 's1', BigInt(0), BigInt(0), BigInt(0))
    s.on_message(msg)
    s.on_message(msg)
    msg.id = BigInt(900)
    s.on_message(msg)

    msg.id = BigInt(1001)
    msg.type = MessageType.none
    s.on_message(msg)

    msg = new VoteResponse(BigInt(1002), 's2', 's1', BigInt(0), false)
    s.on_message(msg)
    msg = new AppendResponse(BigInt(1003), 's2', 's1', BigInt(0), false)
    me.on_message_cb(msg)
}

function unique_id_pooling() {
    console.log('unique id pooling')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(0)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me, BigInt(2))
    s.start_server()
    s.promote_to_leader()
    s.on_client_request(Buffer.from('test data'))
    s.on_client_request(Buffer.from('test data'))
    s.on_client_request(Buffer.from('test data'))
    s.on_client_request(Buffer.from('test data'))
    s.on_client_request(Buffer.from('test data'))
}

function timeout_messages() {
    console.log('timeout messages')
    const ev = new EventLog()
    const me = new TestMessagingEngine(ev)
    const se = new TestStorageEngine(ev)
    const te = new TestTimeoutEngine(ev)

    se.kv.set('message_id_chunk', BigInt(42000000).toString())
    const state = new State()
    state.commit_idx = BigInt(0)
    state.current_term = BigInt(42)
    state.peer_addresses = ['s1', 's2', 's3', 's4']
    state.voted_for = 's2'
    se.kv.set('state', state.toString())

    const s = new Server('s1', ['s1', 's2', 's3'], se, te, me, BigInt(2))
    s.start_server()
    ev.logs[3].args.callback()
    for ( const [name, cb] of te.timeouts) {
        cb()
    }
    te.timeouts = new Map()
    s.promote_to_leader()
    s.on_client_request(Buffer.from('test data'))
    for ( const [name, cb] of te.timeouts) {
        cb()
    }
}

build_from_scratch()

build_from_state()

make_invalid_state()

become_candidate()

candidate_become_leader()

candidate_step_down()

accept_vote_request()

deny_vote_request()

client_request()

append_request()

append_response()

append_to_trailing_peer()

trigger_heartbeat()

find_common_log()

various_invalid_messages()

unique_id_pooling()

timeout_messages()
