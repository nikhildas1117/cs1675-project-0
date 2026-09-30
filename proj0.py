#!/usr/bin/env python3
"""Run the woonsocket client/server sweep in local Docker containers.

Builds a local image (from the repo's Dockerfile), compiles the binaries into a bind-mounted target/ directory, and then runs the server and client as sibling containers on a private Docker network for each leg of the sweep.

Usage: ./proj0.py <closed-loop|open-loop> <work-kind>
  e.g. ./proj0.py closed-loop immediate
       ./proj0.py open-loop const:100
       ./proj0.py open-loop poisson:100
       ./proj0.py open-loop payload
"""

import argparse
import datetime
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
# names of Docker resources (image, container network, container names)
IMAGE = "woonsocket-project0"
NETWORK = "woonsocket-net"
SERVER_NAME = "woonsocket-server"
CLIENT_NAME = "woonsocket-client"
# Run as the host user so bind-mounted output files aren't left root-owned.
CONTAINER_USER = f"{os.getuid()}:{os.getgid()}"

# runtime configuration
PORT = 4242
SERVER_RUNTIME_SECS = 15
CLIENT_RUNTIME_SECS = 15
TIMEOUT_SECS = 20

CLOSED_LOOP_THREAD_COUNTS = [1, 2, 4, 8, 16, 32, 64, 128]
OPEN_LOOP_INTERVALS_US = [128, 64, 32, 16, 8, 4, 2, 1]

def run(cmd, **kwargs):
    print(f"$ {' '.join(cmd)}")
    kwargs.setdefault("check", True)
    return subprocess.run(cmd, **kwargs)


def rm_container(name):
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def commit_hash():
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "nogit"


def build_image():
    run(["docker", "build", "-t", IMAGE, "-f", str(REPO_ROOT / "Dockerfile"), str(REPO_ROOT)])


def ensure_network():
    exists = subprocess.run(
        ["docker", "network", "inspect", NETWORK],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if exists.returncode != 0:
        run(["docker", "network", "create", NETWORK])


def build_binaries():
    run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            CONTAINER_USER,
            "-v",
            f"{REPO_ROOT}:/repo",
            "-w",
            "/repo",
            IMAGE,
            "bash",
            "-c",
            "time cargo build --release --bin server --bin client",
        ]
    )


def server_port_open():
    result = subprocess.run(
        ["docker", "exec", SERVER_NAME, "bash", "-c", f"ss -tulpn | grep {PORT}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def start_server(run_id, data_prefix):
    out_dir = f"runs/{run_id}/server-data/{data_prefix}"
    command = (
        f"mkdir -p {out_dir} && "
        f"timeout {TIMEOUT_SECS} ./target/release/server --port {PORT} "
        f"--runtime-secs {SERVER_RUNTIME_SECS} "
        f"--outpath {out_dir} > {out_dir}/server-stdout 2> {out_dir}/server-stderr"
    )
    print(f"Executing: {command}")
    run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            SERVER_NAME,
            "--user",
            CONTAINER_USER,
            "--network",
            NETWORK,
            "-v",
            f"{REPO_ROOT}:/repo",
            "-w",
            "/repo",
            IMAGE,
            "bash",
            "-c",
            command,
        ]
    )


def run_client(run_id, work_kind, data_prefix, client_args):
    out_dir = f"runs/{run_id}/client-data/{data_prefix}"
    command = (
        f"mkdir -p {out_dir} && "
        f"SERVER_IP=$(getent hosts {SERVER_NAME} | awk '{{print $1}}') && "
        f" timeout {TIMEOUT_SECS} "
        f" ./target/release/client "
        f" --ip $SERVER_IP --port {PORT} "
        f" --work {work_kind} --outpath {out_dir} "
        f" {client_args} "
        f" > {out_dir}/client-stdout 2> {out_dir}/client-stderr"
    )
    print(f"Executing: {command}")
    run(
        [
            "docker",
            "run",
            "--rm",
            "--name",
            CLIENT_NAME,
            "--user",
            CONTAINER_USER,
            "--network",
            NETWORK,
            "-v",
            f"{REPO_ROOT}:/repo",
            "-w",
            "/repo",
            IMAGE,
            "bash",
            "-c",
            command,
        ]
    )


def run_iter(run_id, work_kind, data_prefix, client_args, wait_for_port):
    start_server(run_id, data_prefix)

    if wait_for_port:
        while not server_port_open():
            print("waiting for server to start")
            time.sleep(2)
    else:
        time.sleep(2)

    run_client(run_id, work_kind, data_prefix, client_args)

    # The server exits on its own once --runtime-secs elapses.
    subprocess.run(["docker", "wait", SERVER_NAME], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("client_kind", choices=["closed-loop", "open-loop-constant", "open-loop-poisson"])
    parser.add_argument("work_kind", help="e.g. immediate, const:100, poisson:100, payload")
    parser.add_argument(
        "--value",
        type=int,
        default=None,
        help=(
            "Run a single closed-loop thread count or open-loop interval (us) "
            "instead of sweeping the default list"
        ),
    )
    args = parser.parse_args()

    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_id = f"{timestamp}-{commit_hash()}"

    build_image()
    ensure_network()
    build_binaries()

    if args.client_kind == "closed-loop":
        thread_counts = [args.value] if args.value is not None else CLOSED_LOOP_THREAD_COUNTS
        for num_threads in thread_counts:
            run_iter(
                run_id,
                args.work_kind,
                f"closed-loop-{num_threads}",
                f"--runtime-secs {CLIENT_RUNTIME_SECS} closed-loop --num-threads {num_threads} ",
                wait_for_port=False,
            )
    else:
        intervals = [args.value] if args.value is not None else OPEN_LOOP_INTERVALS_US
        kind = "constant" if args.client_kind == "open-loop-constant" else "poisson"
        for interval in intervals:
            run_iter(
                run_id,
                args.work_kind,
                f"open-loop-{kind}-{interval}",
                f"--runtime-secs {CLIENT_RUNTIME_SECS} open-loop --interval-us {interval} --kind {kind} ",
                wait_for_port=True,
            )

    print(f"Done. Output data is under project-0/runs/{run_id}/")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"Command failed: {exc}", file=sys.stderr)
        rm_container(SERVER_NAME)
        rm_container(CLIENT_NAME)
        sys.exit(1)
    except KeyboardInterrupt:
        rm_container(SERVER_NAME)
        rm_container(CLIENT_NAME)
        sys.exit(1)