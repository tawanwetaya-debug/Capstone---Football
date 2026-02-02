# src/extract/base/api_limits.py

from src.extract.base.rate_limiter import RateLimiter

# Short-term (burst)
API_SPORTS_MINUTE_LIMITER = RateLimiter(
    calls=100,
    period_seconds=10
)

# Long-term (daily quota)
API_SPORTS_DAILY_LIMITER = RateLimiter(
    calls=5000,              # เผื่อ buffer
    period_seconds=86400     # 24 hours
)

