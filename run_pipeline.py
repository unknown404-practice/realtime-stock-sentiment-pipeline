#!/usr/bin/env python3
"""
Unified Stock Sentiment Pipeline Runner
=======================================
Launches both the Kafka producer (producer.py) and FinBERT consumer (consumer.py)
as concurrent child processes with synchronized signal handling and graceful shutdown.
"""

import os
import sys
import time
import signal
import argparse
import subprocess

# Ensure robust UTF-8 printing on Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def parse_args():
    parser = argparse.ArgumentParser(description="Unified Stock Sentiment Pipeline Runner")
    parser.add_argument(
        "--producer-only",
        action="store_true",
        help="Launch only the stock news producer"
    )
    parser.add_argument(
        "--consumer-only",
        action="store_true",
        help="Launch only the FinBERT sentiment consumer"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.8,
        help="Producer publish interval in seconds (default: 0.8s)"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum messages to produce and consume (0 = infinite)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="stock-news",
        help="Kafka topic (default: stock-news)"
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        default=None,
        help="Optional MongoDB connection URI override"
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=None,
        help="Optional MongoDB database name"
    )
    parser.add_argument(
        "--collection-name",
        type=str,
        default=None,
        help="Optional MongoDB sentiment collection name"
    )
    parser.add_argument(
        "--logs-collection",
        type=str,
        default=None,
        help="Optional MongoDB pipeline logs collection name"
    )
    return parser.parse_args()


def terminate_process_safely(proc: subprocess.Popen, name: str, timeout: float = 5.0):
    """Gracefully terminate a child process with a grace period, escalating to kill if necessary."""
    if proc is None or proc.poll() is not None:
        return

    print(f"[Pipeline Runner] Waiting for {name} (PID: {proc.pid}) to exit cleanly...", flush=True)
    # 1. Allow a brief grace period for child process finally blocks to complete
    try:
        proc.wait(timeout=2.0)
        print(f"[Pipeline Runner] {name} exited cleanly with code {proc.returncode}.", flush=True)
        return
    except subprocess.TimeoutExpired:
        pass

    # 2. On Windows, terminate the entire process tree to prevent orphaned stub children
    if sys.platform == "win32":
        print(f"[Pipeline Runner] Terminating {name} process tree (PID: {proc.pid})...", flush=True)
        try:
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True)
            proc.wait(timeout=2.0)
            print(f"[Pipeline Runner] {name} process tree terminated cleanly.", flush=True)
            return
        except Exception:
            pass

    # 3. Standard fallback termination
    print(f"[Pipeline Runner] Sending termination signal to {name}...", flush=True)
    try:
        proc.terminate()
        proc.wait(timeout=timeout)
        print(f"[Pipeline Runner] {name} exited with code {proc.returncode}.", flush=True)
    except subprocess.TimeoutExpired:
        print(f"[Pipeline Runner Warning] {name} did not exit within {timeout}s; killing process...", flush=True)
        try:
            proc.kill()
            proc.wait(timeout=2.0)
        except Exception:
            pass


def main():
    args = parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else "."
    producer_script = os.path.join(base_dir, "producer.py")
    consumer_script = os.path.join(base_dir, "consumer.py")

    launch_producer = not args.consumer_only
    launch_consumer = not args.producer_only

    if not launch_producer and not launch_consumer:
        print("[Pipeline Runner Error] Both --producer-only and --consumer-only specified.", flush=True)
        sys.exit(1)

    print("=" * 80, flush=True)
    print("[+] Unified Stock Sentiment Streaming Pipeline Runner", flush=True)
    print(f"* Python Interpreter : {sys.executable}", flush=True)
    print(f"* Kafka Broker       : {args.bootstrap_servers}", flush=True)
    print(f"* Kafka Topic        : {args.topic}", flush=True)
    print(f"* Run Producer       : {launch_producer} (interval: {args.interval:.1f}s)", flush=True)
    print(f"* Run Consumer       : {launch_consumer} (FinBERT local inference)", flush=True)
    print(f"* Max Messages       : {'Infinite' if args.max_messages == 0 else args.max_messages}", flush=True)
    print("* Press Ctrl+C in this terminal to stop all pipeline workers gracefully.", flush=True)
    print("=" * 80, flush=True)

    producer_proc = None
    consumer_proc = None
    shutting_down = False
    exit_error = False

    def handle_signal(signum, frame):
        nonlocal shutting_down
        if not shutting_down:
            shutting_down = True
            print("\n[Pipeline Runner] Interrupt received. Terminating all workers...", flush=True)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        # 1. Start Consumer first (so it is ready when messages arrive)
        if launch_consumer:
            consumer_cmd = [
                sys.executable,
                "-u",
                consumer_script,
                "--bootstrap-servers", args.bootstrap_servers,
                "--topic", args.topic,
                "--max-messages", str(args.max_messages)
            ]
            if args.mongo_uri:
                consumer_cmd.extend(["--mongo-uri", args.mongo_uri])
            if args.db_name:
                consumer_cmd.extend(["--db-name", args.db_name])
            if args.collection_name:
                consumer_cmd.extend(["--collection-name", args.collection_name])
            if args.logs_collection:
                consumer_cmd.extend(["--logs-collection", args.logs_collection])

            print(f"[Pipeline Runner] Spawning FinBERT Consumer process...", flush=True)
            consumer_proc = subprocess.Popen(consumer_cmd, cwd=base_dir)

            # Give the consumer a brief moment to initialize model and subscribe
            time.sleep(5.0)

        # 2. Start Producer
        if launch_producer:
            producer_cmd = [
                sys.executable,
                "-u",
                producer_script,
                "--bootstrap-servers", args.bootstrap_servers,
                "--topic", args.topic,
                "--interval", str(args.interval),
                "--max-messages", str(args.max_messages)
            ]
            print(f"[Pipeline Runner] Spawning Stock News Producer process...", flush=True)
            producer_proc = subprocess.Popen(producer_cmd, cwd=base_dir)

        print("[Pipeline Runner] All requested streaming workers are running.", flush=True)
        print("-" * 80, flush=True)

        # 3. Monitor child processes
        producer_done_time = None
        while not shutting_down:
            time.sleep(0.5)

            # Check if producer exited
            if producer_proc is not None and producer_proc.poll() is not None:
                p_code = producer_proc.poll()
                if p_code != 0 and not shutting_down:
                    print(f"\n[Pipeline Runner] Producer exited unexpectedly with code {p_code}.", flush=True)
                    shutting_down = True
                    exit_error = True
                    break
                elif args.max_messages == 0 and not shutting_down:
                    print(f"\n[Pipeline Runner] Producer stopped.", flush=True)
                    shutting_down = True
                    break
                elif args.max_messages > 0 and producer_done_time is None:
                    producer_done_time = time.time()
                    print(f"\n[Pipeline Runner] Producer finished emitting {args.max_messages} messages. Waiting for consumer to drain...", flush=True)

            # Check if consumer exited
            if consumer_proc is not None and consumer_proc.poll() is not None:
                c_code = consumer_proc.poll()
                if c_code != 0 and not shutting_down:
                    print(f"\n[Pipeline Runner] Consumer exited unexpectedly with code {c_code}.", flush=True)
                    shutting_down = True
                    exit_error = True
                    break
                elif args.max_messages == 0 and not shutting_down:
                    print(f"\n[Pipeline Runner] Consumer stopped.", flush=True)
                    shutting_down = True
                    break

            # If both were asked to process a finite number of messages and both are done
            if args.max_messages > 0:
                p_done = (producer_proc is None or producer_proc.poll() is not None)
                c_done = (consumer_proc is None or consumer_proc.poll() is not None)
                if p_done and c_done:
                    print(f"\n[Pipeline Runner] Both workers completed processing {args.max_messages} messages.", flush=True)
                    break
                # If producer finished and consumer has drained all available messages
                if p_done and producer_done_time is not None:
                    if time.time() - producer_done_time > 15.0:
                        print("\n[Pipeline Runner] Ingest queue drained after producer completion. Exiting cleanly.", flush=True)
                        break

    except KeyboardInterrupt:
        print("\n[Pipeline Runner] KeyboardInterrupt caught.", flush=True)
    finally:
        print("\n[Pipeline Runner] Initiating shutdown sequence...", flush=True)
        if producer_proc is not None:
            terminate_process_safely(producer_proc, "Producer", timeout=5.0)
        if consumer_proc is not None:
            terminate_process_safely(consumer_proc, "Consumer", timeout=5.0)
        print("[Pipeline Runner] All workers terminated. Pipeline shutdown complete.", flush=True)
        sys.exit(1 if exit_error else 0)


if __name__ == "__main__":
    main()
