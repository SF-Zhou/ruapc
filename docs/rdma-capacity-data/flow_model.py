#!/usr/bin/env python3
"""Finite flow-control model for the minimum ACK-of-ACK threshold change.

Run with Python 3; the JSON report is written to stdout. No RDMA device or
third-party package is used. This models successful FIFO delivery, eventual
local completion (represented as already settled), a one-entry data window,
two standalone-ACK credits, and arbitrary interleavings of direct data sends,
ACK sends and delivery. There are no additional activations after the initial
seeds, no keepalive timers, no posting errors, and no piggybacked ACKs. These
assumptions bound the result; this is not an exhaustive model of the transport.

For W=1/A=2, thresholds 1 and 2 both have no data-credit deadlock from seeds
(0,0), (1,0), (1,1), (2,0), or (2,1). Threshold 1 nevertheless sustains useless
pure-ACK ping-pong after one activation; threshold 2 lets that traffic stop.

The pre-existing boundary (2,2), where both sides already hold two unconfirmed
ACKs, deadlocks with either threshold. Long stalls spanning keepalive emissions
can create this boundary and are outside the finite-seed liveness conclusion.
It must not be presented as a newly proved deadlock-free keepalive protocol.
"""

from collections import deque
import json


def transitions(state, threshold):
    # Endpoint tuple: unconfirmed DATA, unconfirmed ACK, received DATA/ACK
    # whose confirmations have not yet been sent. Local completion is settled.
    peers = [list(state[0]), list(state[1])]
    queues = [list(state[2]), list(state[3])]
    for sender in (0, 1):
        receiver = 1 - sender
        data, acks, pending_data, pending_acks = peers[sender]
        if data < 1:
            changed = [peer[:] for peer in peers]
            pending = [queue[:] for queue in queues]
            changed[sender][0] += 1
            pending[sender].append((0, 0, 0))
            yield pack(changed, pending)
        if (pending_data >= 1 or pending_acks >= threshold) and acks < 2:
            changed = [peer[:] for peer in peers]
            pending = [queue[:] for queue in queues]
            changed[sender][1] += 1
            changed[sender][2] = changed[sender][3] = 0
            pending[sender].append((1, pending_data, pending_acks))
            yield pack(changed, pending)
        if queues[sender]:
            changed = [peer[:] for peer in peers]
            pending = [queue[:] for queue in queues]
            kind, data_confirmed, acks_confirmed = pending[sender].pop(0)
            changed[receiver][0] -= data_confirmed
            changed[receiver][1] -= acks_confirmed
            changed[receiver][2 + kind] += 1
            yield pack(changed, pending)


def pack(peers, queues):
    assert all(value >= 0 for peer in peers for value in peer)
    return (tuple(peers[0]), tuple(peers[1]), tuple(queues[0]), tuple(queues[1]))


def explore(threshold, seeds):
    initial = (
        (0, seeds[0], 0, 0),
        (0, seeds[1], 0, 0),
        ((1, 0, 0),) * seeds[0],
        ((1, 0, 0),) * seeds[1],
    )
    todo = deque([initial])
    seen = {initial}
    deadlocks = []
    while todo:
        state = todo.popleft()
        successors = list(transitions(state, threshold))
        if not successors:
            deadlocks.append(state)
        for successor in successors:
            if successor not in seen:
                seen.add(successor)
                todo.append(successor)
    return {
        "ack_threshold": threshold,
        "initial_unconfirmed_acks": seeds,
        "reachable_states": len(seen),
        "data_credit_deadlocks": len(deadlocks),
        "deadlock_states": deadlocks,
    }


def main():
    reports = []
    for seeds in [(0, 0), (1, 0), (1, 1), (2, 0), (2, 1), (2, 2)]:
        for threshold in (1, 2):
            result = explore(threshold, seeds)
            expected = 1 if seeds == (2, 2) else 0
            assert result["data_credit_deadlocks"] == expected, result
            reports.append(result)
    print(json.dumps({"window": 1, "ack_limit": 2, "scenarios": reports}, indent=2))


if __name__ == "__main__":
    main()
