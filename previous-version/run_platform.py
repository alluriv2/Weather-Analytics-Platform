import os
import signal
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent

# Uses the Python from the currently activated virtual environment.
PYTHON = Path(sys.executable)

processes: list[subprocess.Popen] = []


def start_process(name: str, command: list[str]) -> subprocess.Popen:
    log_path = PROJECT_ROOT / f"{name}.log"

    log_file = open(
        log_path,
        "a",
        encoding="utf-8",
    )

    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    processes.append(process)

    print(
        f"Started {name:<12} "
        f"PID={process.pid} "
        f"log={log_path.name}"
    )

    return process


def stop_processes():
    print("\nStopping application processes...")

    for process in processes:
        if process.poll() is None:
            try:
                os.killpg(
                    os.getpgid(process.pid),
                    signal.SIGTERM,
                )
            except ProcessLookupError:
                pass

    deadline = time.time() + 10

    for process in processes:
        if process.poll() is None:
            remaining = max(
                0,
                deadline - time.time(),
            )

            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(
                        os.getpgid(process.pid),
                        signal.SIGKILL,
                    )
                except ProcessLookupError:
                    pass


def check_python():
    if not PYTHON.exists():
        print(
            f"Python interpreter not found:\n{PYTHON}"
        )
        sys.exit(1)


def start_docker():
    print("Starting Docker services...")

    subprocess.run(
        [
            "docker",
            "compose",
            "up",
            "-d",
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def wait_for_postgres():
    print("Waiting for PostgreSQL...")

    for _ in range(30):
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Health.Status}}",
                "weather-postgres",
            ],
            capture_output=True,
            text=True,
        )

        if (
            result.returncode == 0
            and result.stdout.strip() == "healthy"
        ):
            print("PostgreSQL is healthy.")
            return

        time.sleep(2)

    raise RuntimeError(
        "PostgreSQL did not become healthy."
    )


def wait_for_kafka():
    print("Waiting for Kafka...")

    # Kafka usually needs a few extra seconds after the container starts.
    time.sleep(10)

    print("Kafka should now be ready.")


def start_application():
    start_process(
        "consumer",
        [
            str(PYTHON),
            "weather_kafka_consumer.py",
        ],
    )

    start_process(
        "producer",
        [
            str(PYTHON),
            "weather_kafka_producer.py",
        ],
    )

    start_process(
        "aggregator",
        [
            str(PYTHON),
            "weather_aggregation_postgres.py",
        ],
    )

    start_process(
        "api",
        [
            str(PYTHON),
            "-m",
            "uvicorn",
            "weather_api:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
        ],
    )

    start_process(
        "dashboard",
        [
            str(PYTHON),
            "-m",
            "dashboard.app",
        ],
    )


def monitor():
    print("\n=======================================")
    print("Weather Platform is running")
    print("=======================================")
    print("Dashboard : http://127.0.0.1:8050")
    print("API Docs  : http://127.0.0.1:8000/docs")
    print("Kafka UI  : http://127.0.0.1:8080")
    print("=======================================\n")
    print("Press Ctrl+C to stop.\n")

    while True:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(
                    f"Process exited unexpectedly. "
                    f"Check {process.args[1:]}"
                )

        time.sleep(2)


def main():
    check_python()

    if not (PROJECT_ROOT / "producer_state.json").exists():
        print(
            "\nproducer_state.json was not found.\n"
            "Run initial_backfill.py once before "
            "starting the platform."
        )
        sys.exit(1)

    try:
        start_docker()
        wait_for_postgres()
        wait_for_kafka()
        start_application()
        monitor()

    except KeyboardInterrupt:
        print("\nShutdown requested.")

    except subprocess.CalledProcessError as exc:
        print(f"\nDocker startup failed:\n{exc}")

    except Exception as exc:
        print(f"\nPlatform error:\n{exc}")

    finally:
        stop_processes()

        print("\nApplication processes stopped.")
        print(
            "Docker containers are still running.\n"
            "Run 'docker compose down' to stop them."
        )


if __name__ == "__main__":
    main()