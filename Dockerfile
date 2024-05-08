FROM ubuntu:22.04

COPY . /bench
WORKDIR /bench

RUN apt-get update && apt-get install -y \
        python3.10 \
        python3-pip \
        curl && \
    pip3 install -r requirements.txt && \
    rm -rf /var/lib/apt/lists/* && \
    (curl https://sh.rustup.rs -sSf | bash -s -- -y) && \
    (echo 'source $HOME/.cargo/env' >> $HOME/.bashrc) && \
    # Download dependencies and pre-build binaries
    ~/.cargo/bin/cargo build --release && \
    ~/.cargo/bin/cargo test --no-run --release
