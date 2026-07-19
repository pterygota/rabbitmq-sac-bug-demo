FROM erlang:27 AS builder
COPY rabbit_stream_sac_coordinator.erl rabbit_stream_sac_coordinator.hrl /build/
RUN erlc -I /build -o /build /build/rabbit_stream_sac_coordinator.erl \
    && ls -la /build/rabbit_stream_sac_coordinator.beam

FROM rabbitmq:4.3.2-management
USER root
COPY --from=builder /build/rabbit_stream_sac_coordinator.beam /tmp/patched.beam
RUN set -eux; \
    RABBIT_EBIN=$(ls -d /opt/rabbitmq/plugins/rabbit-*/ebin | head -1); \
    cp /tmp/patched.beam "$RABBIT_EBIN/rabbit_stream_sac_coordinator.beam"; \
    rm /tmp/patched.beam
USER rabbitmq
