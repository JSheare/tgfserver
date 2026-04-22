"""A module containing a child class of multiprocessing.Process that has been modified to be async compatible."""
import asyncio
import multiprocessing


class AsyncProcess(multiprocessing.Process):
    """A class that modifies multiprocessing.Process to make it async compatible."""
    async def join(self, timeout: float | None = None) -> None:
        task = asyncio.to_thread(super().join, timeout)
        await task
