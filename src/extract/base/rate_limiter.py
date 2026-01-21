import time
from dataclasses import dataclass
from functools import wraps

@dataclass
class RateLimiter:
    calls: int
    period_seconds: float
    _tokens: float = 0.0
    _last: float = 0.0

    def __post_init__(self):
        self._tokens = self.calls
        self._last = time.monotonic()

    def wait(self):
        now = time.monotonic()
        elapsed = now - self._last
        self._last = now

        # refill tokens
        refill = elapsed * (self.calls / self.period_seconds)
        self._tokens = min(self.calls, self._tokens + refill)

        if self._tokens < 1:
            # need to wait until we have 1 token
            needed = 1 - self._tokens
            sleep_s = needed * (self.period_seconds / self.calls)
            time.sleep(sleep_s)
            self._tokens = 0  # will become ~1 after sleep and next refill
        else:
            self._tokens -= 1

def rate_limited(limiter):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            limiter.wait()
            return fn(*args, **kwargs)
        return wrapper
    return deco