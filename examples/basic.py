"""Minimal end-to-end demo.

Run a Redis on localhost:6379, then:  python examples/basic.py
"""
import asyncio

from toro import Queue, Worker


async def main():
    queue = Queue("emails")

    # Producer: enqueue a few jobs, one delayed, one that will fail-and-retry.
    await queue.add("welcome", {"to": "ada@example.com"})
    await queue.add("welcome", {"to": "alan@example.com"}, delay=2000)
    await queue.add("flaky", {"n": 1}, attempts=3, backoff={"type": "exponential", "delay": 500})

    # Consumer.
    async def process(job):
        if job.name == "flaky" and job.attempts_made < 3:
            raise RuntimeError(f"transient failure (try {job.attempts_made})")
        print(f"  processed {job.name} #{job.id} -> {job.data}")
        return {"ok": True}

    worker = Worker("emails", process, concurrency=4)
    worker.on("completed", lambda job, res: print(f"  ✓ completed {job.name} #{job.id}"))
    worker.on("retrying", lambda job, exc: print(f"  ↻ retrying #{job.id}: {exc}"))
    worker.on("failed", lambda job, exc: print(f"  ✗ failed #{job.id}: {exc}"))

    runner = asyncio.create_task(worker.run())

    await asyncio.sleep(5)  # let the delayed + retried jobs flush
    print("counts:", await queue.counts())

    await worker.stop()
    runner.cancel()
    await queue.close()


if __name__ == "__main__":
    asyncio.run(main())
