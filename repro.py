# /// script
# requires-python = ">=3.11"
# dependencies = ["rstream>=0.24"]
# ///
"""
Minimal reproduction of a RabbitMQ Streams SAC coordinator bug.

Symptom: when several single-active-consumer instances subscribe to a
super stream at the same time, some partitions end up with no client
actively consuming, even though the broker reports the group as healthy
(one member "active (connected)", the rest "waiting"). Delivery on those
partitions never resumes until a consumer restarts.

Trigger conditions (all present in ordinary production topologies):
- A super stream with multiple partitions.
- Multiple SAC consumers subscribing concurrently.
- The consumer_update_listener responding with any non-trivial latency
  (e.g. a `query_offset` round-trip to resume from stored offset -- the
  documented pattern for durable resume).

The bug is in the broker's SAC coordinator. Under stock RabbitMQ 4.3.2
this script fails (some partitions never deliver). Against the patched
image built from `../tests/Messaging.RabbitMQ.Stream.IntegrationTests/broker-patch/`
this script succeeds.

Usage:
    python3 repro.py                     # localhost:5552
    python3 repro.py --host X --port N   # override
Exit 0 on success, 1 on stranded partitions.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from collections import Counter

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    EventContext,
    MessageContext,
    OffsetNotFound,
    OffsetSpecification,
    OffsetType,
    RouteType,
    SuperStreamConsumer,
    SuperStreamCreationOption,
    SuperStreamProducer,
    amqp_decoder,
)


PARTITIONS = 20
CONSUMERS = 3
MESSAGES = 300
CONSUMER_NAME = "sac-repro"
STRANDED_WINDOW_SEC = 45.0

logger = logging.getLogger("sac-repro")


async def routing_extractor(message: AMQPMessage) -> str:
    return message.application_properties["key"]


async def preload_messages(super_stream: str, host: str, port: int) -> None:
    creation = SuperStreamCreationOption(n_partitions=PARTITIONS)
    async with SuperStreamProducer(
        host=host,
        port=port,
        # load_balancer_mode forces every connection through the given
        # host:port, ignoring the per-partition addresses the broker
        # advertises via metadata. Needed here because we bind the container
        # on a random host port but the broker advertises "localhost:5552".
        load_balancer_mode=True,
        username="guest",
        password="guest",
        routing_extractor=routing_extractor,
        super_stream=super_stream,
        super_stream_creation_option=creation,
        routing=RouteType.Hash,
    ) as producer:
        for i in range(MESSAGES):
            await producer.send(
                AMQPMessage(
                    body=f"msg-{i}".encode(),
                    application_properties={"key": f"key-{i}"},
                )
            )


async def consumer_update_listener(is_active: bool, ctx: EventContext) -> OffsetSpecification:
    # The query_offset round-trip is what widens the race window: while the
    # client is talking to the server to fetch the stored offset, a second
    # consumer's registration can trigger a rebalance that puts this consumer
    # into DEACTIVATING state. That is the precondition for the coordinator
    # bug this script reproduces.
    if is_active:
        try:
            offset = await ctx.consumer.query_offset(
                stream=ctx.stream, subscriber_name=CONSUMER_NAME
            )
            return OffsetSpecification(OffsetType.OFFSET, offset)
        except OffsetNotFound:
            return OffsetSpecification(OffsetType.OFFSET, 0)
    return OffsetSpecification(OffsetType.OFFSET, 0)


async def start_consumer(
    idx: int,
    super_stream: str,
    host: str,
    port: int,
    received: Counter,
) -> tuple[SuperStreamConsumer, asyncio.Task]:
    consumer = SuperStreamConsumer(
        host=host,
        port=port,
        # See SuperStreamProducer above -- required because of the random
        # host-port binding vs the broker's advertised address.
        load_balancer_mode=True,
        vhost="/",
        username="guest",
        password="guest",
        super_stream=super_stream,
    )

    async def on_message(msg: AMQPMessage, ctx: MessageContext) -> None:
        received[(idx, ctx.stream)] += 1

    await consumer.subscribe(
        callback=on_message,
        offset_specification=ConsumerOffsetSpecification(offset_type=OffsetType.FIRST),
        decoder=amqp_decoder,
        properties={
            "single-active-consumer": "true",
            "name": CONSUMER_NAME,
            "super-stream": super_stream,
        },
        subscriber_name=CONSUMER_NAME,
        consumer_update_listener=consumer_update_listener,
    )
    task = asyncio.create_task(consumer.run(), name=f"consumer-{idx}")
    return consumer, task


async def main(host: str, port: int) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    # rstream logs a spurious traceback from its background producer timer when
    # the `async with SuperStreamProducer` context exits (a race between the
    # timer flushing buffered messages and close()). It has no effect on the
    # 300 preloaded messages -- they all land before the context exits -- so
    # keep it out of the demo output.
    logging.getLogger("rstream.producer").setLevel(logging.CRITICAL)

    super_stream = f"sac-repro-{int(time.time())}"

    logger.info(
        "preloading %d hash-routed messages to super stream %s (%d partitions)",
        MESSAGES,
        super_stream,
        PARTITIONS,
    )
    await preload_messages(super_stream, host, port)

    received: Counter = Counter()

    logger.info("starting %d SAC consumers concurrently -- this is the bug trigger", CONSUMERS)
    started = await asyncio.gather(
        *[start_consumer(i, super_stream, host, port, received) for i in range(CONSUMERS)]
    )
    consumers, tasks = zip(*started)

    expected = {f"{super_stream}-{i}" for i in range(PARTITIONS)}
    logger.info("waiting up to %.0fs for all %d partitions to deliver", STRANDED_WINDOW_SEC, PARTITIONS)

    deadline = time.monotonic() + STRANDED_WINDOW_SEC
    while time.monotonic() < deadline:
        seen = {s for (_, s) in received}
        if seen >= expected:
            break
        await asyncio.sleep(0.25)

    seen = {s for (_, s) in received}
    stranded = sorted(expected - seen)
    result = 0
    if stranded:
        logger.error(
            "STRANDED: %d of %d partitions never delivered within %.0fs",
            len(stranded),
            PARTITIONS,
            STRANDED_WINDOW_SEC,
        )
        logger.error("  stranded partitions: %s", stranded)
        for c in range(CONSUMERS):
            partitions = sorted({s for (i, s) in received if i == c})
            total = sum(v for (i, _), v in received.items() if i == c)
            logger.error(
                "  consumer[%d]: received=%d from %d partitions %s",
                c,
                total,
                len(partitions),
                partitions,
            )
        result = 1
    else:
        logger.info("SUCCESS: all %d partitions delivered messages", PARTITIONS)

    logger.info("shutting down")
    await asyncio.gather(*(c.close() for c in consumers), return_exceptions=True)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5552)
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.host, args.port)))
