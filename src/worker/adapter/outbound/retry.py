"""Bounded retries with exponential backoff and full jitter (Annex D rule 7): the wait before retry
`n` is a random value between 0 and `base * 2**n` (base 200 ms: up to 400 ms before the second
attempt, up to 800 ms before the third), so processes that failed together do not hit again
together. Only the errors `retryable` accepts are retried; anything else is raised at once."""

import random
import time
from collections.abc import Callable


def retrying[T](
    call: Callable[[], T],
    attempts: int,
    retryable: Callable[[Exception], bool],
    backoff_seconds: float = 0.2,
    sleep: Callable[[float], None] = time.sleep,
    rand: Callable[[float, float], float] = random.uniform,
    on_retry: Callable[[int, Exception, float], None] | None = None,
) -> T:
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            return call()
        except Exception as error:
            if attempt >= attempts or not retryable(error):
                raise
            wait = rand(0, backoff_seconds * 2**attempt)
            if on_retry is not None:
                on_retry(attempt, error, wait)
            sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover
