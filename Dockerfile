# Shopping Sessions: Rust pipeline.
# Stage 1 compiles the binary for the scorer's Intel Xeon (linux/amd64, AVX2).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
# The unit tests check the fast parsers against simple reference versions on
# this machine's CPU (including the AVX2 scanner), so a mismatch fails the build.
RUN cargo test --release --locked && cargo build --release --locked

# Stage 2 is a small image with only the binary. At run time: read-only root,
# uid 1000, no network; the program reads /data and writes /output only.
FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
CMD ["/app/shopping-sessions"]
