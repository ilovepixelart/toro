# Examples

Runnable, self-contained demos of toro. Each needs a Redis on `localhost:6379`:

```bash
docker run --rm -p 6379:6379 redis:7-alpine   # or any local Redis
```

Then run from the repo root:

| Example | What it shows |
|---|---|
| [`basic.py`](basic.py) | The end-to-end loop - enqueue jobs (including a delayed one and a flaky one with `attempts` + exponential backoff), process them with a `concurrency=4` worker, and react to `completed` / `retrying` / `failed` events. |
| [`stalled.py`](stalled.py) | Crash recovery - a "zombie" worker grabs a job and hangs; a healthy worker detects the stalled job, requeues it, and completes it **exactly once**. The zombie's late finish is rejected by its lock token. |

```bash
python examples/basic.py
python examples/stalled.py
```
