import asyncio
import time
from typing import Any
from urllib.parse import urlparse

import aiohttp


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args: Any, **kwds: Any) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwds)
        return cls._instances[cls]


class AsyncHTTPClient(SingletonMeta):
    def __init__(self, concurrency: int = 8, max_retries: int = 3, timeout: int | None = None) -> None:
        self._sem = asyncio.Semaphore(concurrency)
        self._session = aiohttp.ClientSession()
        self._hosts_rate_limits = {}
        self._last_requests: dict[str, float] = {}
        self._host_locks: dict[str, asyncio.Lock] = {}
        self._max_retries = max_retries
        self._timeout = aiohttp.ClientTimeout(total=timeout) if timeout else None
        self._default_rate_limit = 0.0
        # TODO: implement logger
        # self.logger = logger

    async def close(self):
        await self._session.close()

    def _host_from_url(self, url: str) -> str:
        return urlparse(url).netloc

    def _get_rate_limits_for_host(self, host: str) -> float:
        return self._hosts_rate_limits.get(host, self._default_rate_limit)

    def _get_lock_for_host(self, host: str) -> asyncio.Lock:
        lock = self._host_locks.get(host)
        if lock is None:
            lock = asyncio.Lock()
            self._host_locks[host] = lock

        return lock

    async def _enforce_rate_limit_wait(self, host: str) -> None:
        rate_limit = self._get_rate_limits_for_host(host)
        if rate_limit <= 0:
            return

        while True:
            lock = self._get_lock_for_host(host)
            async with lock:
                last = self._last_requests.get(host, 0.0)
                now = time.monotonic()
                remaining = rate_limit - (last - now)
                if remaining <= 0:
                    self._last_requests[host] = now
                    return

            await asyncio.sleep(remaining)

    async def _request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        host = self._host_from_url(url)
        await self._enforce_rate_limit_wait(host)

        async with self._sem:
            for attempt in range(self._max_retries):
                try:
                    headers = kwargs.get("headers", {})
                    kwargs["headers"] = headers

                    response = await self._session.request(method, url, **kwargs)
                    response.raise_for_status()
                    self._last_requests[host] = time.monotonic()
                    return response
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    # self.logger.debut(f"Request to {url} failed on attempt {attempt} with exception: {e}")
                    await asyncio.sleep(2**attempt)
            raise RuntimeError

    async def get_json(self, url: str, params: dict | None, headers: dict | None, **kwargs) -> Any:
        response = await self._request("GET", url, params=params, headers=headers, **kwargs)
        return await response.json()

    async def get_text(self, url: str, params: dict | None, headers: dict | None, **kwargs) -> str:
        response = await self._request("GET", url, params=params, headers=headers, **kwargs)
        return await response.text()
