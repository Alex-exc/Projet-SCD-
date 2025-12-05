FROM elixir:1.15-alpine

RUN apk add --no-cache build-base git

WORKDIR /app

COPY mix.exs mix.lock ./
RUN mix do deps.get, deps.compile

COPY lib ./lib

ENV ELIXIR_ERL_OPTIONS "-name ${NODE_NAME} -setcookie ${COOKIE}"

CMD ["mix", "run", "--no-halt"]
