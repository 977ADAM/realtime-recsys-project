import asyncio
import logging
import os
import signal
from contextlib import suppress

if __package__:
    from .db import init_db
    from .kafka import KafkaConsumerWorker
    from .prom_metrics import start_worker_metrics_http_server
else:  # pragma: no cover - fallback for direct script execution
    from db import init_db
    from kafka import KafkaConsumerWorker
    from prom_metrics import start_worker_metrics_http_server


logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def _run_worker() -> None:
    # Keep worker process self-sufficient on startup.
    await asyncio.to_thread(init_db)
    start_worker_metrics_http_server()

    worker = KafkaConsumerWorker(enabled=True)
    await worker.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    logger.info("Kafka worker service is running")
    await stop_event.wait()
    logger.info("Stop signal received, shutting down Kafka worker service")
    await worker.stop()


def main() -> None:
    _configure_logging()
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
