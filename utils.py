from typing import AsyncIterator


async def async_chunks[T](
    async_iterator: AsyncIterator[T], size: int
) -> AsyncIterator[list[T]]:
    finished = False

    while not finished:
        results: list[T] = []

        for _ in range(size):
            try:
                result = await anext(async_iterator)
            except StopAsyncIteration:
                finished = True
            else:
                results.append(result)

        if results:
            yield results
