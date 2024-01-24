FROM rust:1.75.0 AS builder
WORKDIR /usr/src/
RUN rustup target add x86_64-unknown-linux-musl

RUN USER=root mkdir yummy-rs
WORKDIR /usr/src/yummy-rs

RUN apt update && apt install musl-tools musl-dev -yq

COPY Cargo.toml ./
COPY yummy ./yummy
COPY yummy-core ./yummy-core
COPY yummy-delta ./yummy-delta
COPY yummy-delta-py ./yummy-delta-py
COPY yummy-features ./yummy-features
COPY yummy-features-py ./yummy-features-py
COPY yummy-llm ./yummy-llm
COPY yummy-ml ./yummy-ml
COPY yummy-ml-py ./yummy-ml-py

WORKDIR /usr/src/yummy-rs/yummy
RUN cargo build --release --features yummy-llm
RUN cargo install --features yummy-llm --target x86_64-unknown-linux-musl --path .
#RUN cargo install --path .

RUN useradd -u 1000 yummy
# Bundle Stage

FROM scratch

COPY --from=builder /usr/local/cargo/bin/yummy .
COPY --from=builder /etc/passwd /etc/passwd
USER yummy
ENTRYPOINT ["./yummy"]

# docker build -t yummyml/yummy:llm -f docker/Dockerfile .
# docker run -it --network app_default -p 8999:8080 --rm -v $(pwd)/tmp/ttt:/home/yummy/.cache/huggingface/hub --name yummy yummyml/yummy:llm llm serve-embeddings --model e5 