# syntax=docker/dockerfile:1.7

ARG ELIXIR_VERSION=1.18.4
ARG OTP_VERSION=28.0
ARG DEBIAN_VERSION=trixie-slim

# BUILD
FROM elixir:${ELIXIR_VERSION} AS build

ENV LANG=C.UTF-8 \
    MIX_ENV=prod

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      curl \
      git \
      openssl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/rebar3 \
    mix local.hex --force && \
    mix local.rebar --force

COPY mix.exs mix.lock ./
COPY config/config.exs config/prod.exs config/

RUN --mount=type=cache,target=/root/.hex \
    --mount=type=cache,target=/root/.cache/rebar3 \
    --mount=type=cache,target=/app/deps \
    --mount=type=cache,target=/app/_build \
    mix deps.get --only ${MIX_ENV} && \
    mix deps.compile

COPY config/runtime.exs config/
COPY lib lib
COPY priv priv
COPY rel rel

RUN --mount=type=cache,target=/root/.hex \
    --mount=type=cache,target=/root/.cache/rebar3 \
    --mount=type=cache,target=/app/deps \
    --mount=type=cache,target=/app/_build \
    mix compile && \
    mix release

# RUNTIME

FROM debian:${DEBIAN_VERSION} AS app

ENV LANG=C.UTF-8 \
    MIX_ENV=prod \
    SHELL=/bin/bash \
    PHX_SERVER=true

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      libstdc++6 \
      openssl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid app --home /app --shell /bin/bash app

COPY --from=build /app/_build/prod/rel/starcite ./starcite
COPY docker-entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh && \
    mkdir -p /var/lib/starcite/raft && \
    chown -R app:app /app /var/lib/starcite

USER app

EXPOSE 4000

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["start"]
