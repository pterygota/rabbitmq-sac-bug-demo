# RabbitMQ Streams SAC bug demo

Race in SAC group step-down process leads to stuck or stranded partitions
that appear to be healthy with active and connected consumers even though 
no messages are sent to any consumers.

Deterministic reproduction of a single-active-consumer (SAC) coordinator
bug in RabbitMQ Streams, in Python against `rabbitmq:4.3.2-management`,
plus a patched broker image built from a one-line source change that
resolves it. Runs end-to-end from Docker + [`uv`][uv]. Also see the
proposed upstream artifacts under [Contents](#contents).

Repro code and this report were created with assistance from various "AI" 
or LLM tools such as Claude code, ChatGPT, and Copilot.

## Symptom

Frequently, when several SAC consumers subscribe to a super stream at 
the same time, some partitions end up with **no client actively consuming**. 
But `rabbitmq-streams list_stream_group_consumers` reports one member
`active (connected)` and the rest `waiting`, indistinguishable from a
healthy group. 

`rabbitmq-streams activate_stream_consumer` does not unstick 
them (re-enters the same buggy coordinator path), and restarting consumer
apps usually does not help and is likely to trigger the bug again. 
The only reliable manual recovery is to force-close every connection on 
the affected partition.

The bug is easily reachable from what I think is a realistic production 
stream SAC workload during connection churn around rollout, with multiple 
pods of a SAC group app deployment starting and stopping around the 
same time, trying to resume work by querying for an offset when they become 
active.

Mostly but not totally effective mitigations include reducing the 
time it takes for a consumer to connect and start consuming by connecting 
to cluster nodes directly as well as controlling the rollout to prevent 
consumers from starting and stopping at the same time.

## Root cause

The general idea is a kind of race in the SAC group consumer step-down
process combined with a missed match on a deactivating consumer.

If other changes to group membership intervene while a step-down
is in flight, the step-down response can be handled by the server in a 
weird way that fails to properly deal with the deactivating consumer, 
(does nothing instead of the effect needed for this case) leaving the 
partition permanently stranded with no messages sent to 
any consumer.

In `deps/rabbit/src/rabbit_stream_sac_coordinator.erl`,
[`apply(#command_activate_consumer{}, ...)`][coord] (v4.3.2 lines
359-408). When the stepping-down consumer's `consumer_update` response
triggers the handler:

1. `lookup_active_consumer/1` returns the stepping-down consumer
   `C0` because `is_active({_, DEACTIVATING})` is defined as `true`
   (line 1180). So `ActCsr = C0`.
2. All connected consumers are set to `{CONNECTED, WAITING}` (`C0`
   goes `DEACTIVATING -> WAITING`).
3. `evaluate_active_consumer/1` picks the new active via
   `PartitionIndex rem length(Consumers)` (line 1421). For partitions
   where that index selects the first consumer in list order, the pick
   is `C0` again.
4. The `?SAME_CSR(Csr, ActCsr)` guard sees "same active consumer as
   before" and emits **no notification** (line 388-392, comment: *"it is
   the same active consumer as before, no need to notify it"*).
5. Coordinator state: `C0` is `{CONNECTED, ACTIVE}`. Reader and client
   state: `C0` is inactive (the reader set `active=false` on its own
   state when it forwarded the step-down). The two views are never
   reconciled and the group is silently stranded.

With 20 partitions and 3 consumers this produces a deterministic pattern:
stranded partitions are always some subset of the ones whose
`PI rem 3 = 0` **and** which subscribed after another consumer's
registration triggered the initial rebalance (in this case typically
`{3, 9, 15}`).

## Fix

A single guard: only skip the notification when the previously-active
consumer's state was truly `{CONNECTED, ACTIVE}`. If it was
`DEACTIVATING`, its client has been told to step down and must be
re-notified when it is re-selected.

```diff
                         Effects =
                             case Csr of
-                                Csr when ?SAME_CSR(Csr, ActCsr) ->
+                                Csr when ?SAME_CSR(Csr, ActCsr) andalso
+                                         ActCsr#consumer.status =:= ?CONN_ACT ->
                                     [];
                                 _ ->
                                     [notify_csr_effect(Csr, S, Name, true)]
                             end,
```

The full diff is `sac-coordinator.patch`, ready to apply against
[upstream `v4.3.2`][coord] with `patch -p1`.


## Demo

Should only require Docker and [`uv`][uv].

```sh
./run.sh stock     # expected: STRANDED, exit 1
./run.sh patched   # expected: SUCCESS, exit 0
```

`run.sh patched` builds `rabbitmq-sac-patched:4.3.2` from the local
`Dockerfile` on first use (two-stage: an `erlang:27` builder compiles
the patched module against upstream headers, then the resulting `.beam`
replaces the shipped one inside `rabbitmq:4.3.2-management`).

### Expected output from stock

```
STRANDED: N of 20 partitions never delivered within 45s
  stranded partitions: [... -3, ... -9, ... -15]      # subset varies
  consumer[0]: received=... from ... partitions [...]
  consumer[1]: received=... from ... partitions [...]
  consumer[2]: received=... from ... partitions [...]
```

Stranded partitions are always drawn from the set where
`PartitionIndex rem 3 = 0`; the exact subset varies with the timing of
consumer registrations within the burst.

### Expected output from patched

```
SUCCESS: all 20 partitions delivered messages
```

Typically within a few hundred milliseconds.

## What `repro.py` does

1. Creates a 20-partition super stream.
2. Preloads 300 hash-routed messages so every partition already has
   deliverable content.
3. Starts three `SuperStreamConsumer`s concurrently
   (`asyncio.gather`). Each consumer's `consumer_update_listener`
   performs a `query_offset` round-trip before responding -- the
   documented durable-resume pattern, and the source of the ~100ms
   window that lets the coordinator race with itself.
4. Waits up to 45 seconds for every partition to deliver at least one
   message. Exits `0` if every partition delivered, `1` with a
   stranded-partition summary otherwise.

## Contents

| File | Purpose |
| --- | --- |
| `repro.py` | Reproduction script. PEP 723 inline metadata declares `rstream>=0.24`. |
| `run.sh` | Orchestrator: starts the requested broker, waits for readiness, runs `repro.py`, tears down. |
| `Dockerfile` | Two-stage build for the patched `rabbitmq-sac-patched:4.3.2` image. |
| `rabbit_stream_sac_coordinator.erl` | Patched module. Base: upstream `v4.3.2`. Only change is the guard on line 389. Original MPL-2.0 header preserved. |
| `rabbit_stream_sac_coordinator.hrl` | Unmodified upstream `v4.3.2` header. Needed to compile the module. |
| `sac-coordinator.patch` | The fix alone as a unified diff against upstream `v4.3.2`. Applies with `patch -p1`. |
| `sac-coordinator-suite.patch` | Regression test as a unified diff against `deps/rabbit/test/rabbit_stream_sac_coordinator_SUITE.erl`. State-machine test with no broker or client required. Fails on unmodified `v4.3.2`; passes with `sac-coordinator.patch` applied. |

[uv]: https://docs.astral.sh/uv/
[coord]: https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.2/deps/rabbit/src/rabbit_stream_sac_coordinator.erl
