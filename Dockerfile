FROM debian:bookworm

# Update default packages
RUN apt-get update -y && apt-get upgrade -y

# Install tools
RUN apt-get install -y linux-perf wget curl build-essential iproute2

# Install rust into world-readable dirs (not /root, which is 700) so the
# toolchain is usable by non-root container users too.
ENV RUSTUP_HOME=/opt/rust
ENV CARGO_HOME=/opt/cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Set path
ENV PATH="/opt/cargo/bin:/usr/lib/linux-tools-6.8.0-51:${PATH}"

# Install flamegraph
RUN cargo install flamegraph

# Let non-root container users read/execute the toolchain, and read/write the
# shared cargo registry cache (needed to fetch deps not already cached above).
RUN chmod -R a+rX /opt/rust && chmod -R a+rwX /opt/cargo