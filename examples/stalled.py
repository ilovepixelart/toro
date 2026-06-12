"""Demo: a worker dies mid-job and another recovers it - exactly one completion.

Run a Redis on localhost:6379, then:  python examples/stalled.py
"""

import asyncio

from toro import Queue, Worker


async def main():
    queue = Queue("recovery_demo")
    # clean slate for the demo
    keys = await queue.redis.keys(queue.keys.base + "*")
    if keys:
        await queue.redis.delete(*keys)

    await queue.add("report", {"id": 42})

    async def hangs(job):
        print(f"  [zombie] picked up #{job.id}, then hangs forever...")
        await asyncio.sleep(30)  # never finishes in time
        return {"by": "zombie"}

    async def works(job):
        print(f"  [healthy] recovered #{job.id}, processing")
        return {"by": "healthy"}

    # A zombie: short lock, never renews, doesn't run stalled checks.
    zombie = Worker(
        "recovery_demo", hangs, lock_duration=500, renew_locks=False, stalled_interval=0
    )
    # A healthy worker that sweeps for stalled jobs every 500ms.
    healthy = Worker("recovery_demo", works, stalled_interval=500, max_stalled_count=3)
    healthy.on("stalled", lambda jid: print(f"  [healthy] detected #{jid} stalled -> requeued"))
    healthy.on("completed", lambda j, r: print(f"  ✓ #{j.id} completed by {r['by']}"))
    zombie.on(
        "lock-lost",
        lambda jid: print(f"  [zombie] woke up, but #{jid} was taken - finish rejected"),
    )

    zt = asyncio.create_task(zombie.run())
    await asyncio.sleep(0.4)  # let the zombie grab the job
    ht = asyncio.create_task(healthy.run())

    await asyncio.sleep(3)
    print("counts:", await queue.counts())

    await zombie.stop()
    await healthy.stop()
    zt.cancel()
    ht.cancel()
    await queue.close()


if __name__ == "__main__":
    asyncio.run(main())
