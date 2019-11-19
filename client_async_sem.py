from urllib.parse import urljoin
import aiohttp
import asyncio

LIMIT = 10


async def fetch(url, session):
    async with session.get(url) as resp:
        return await resp.read()

async def bound_fetch(url, session, sem):
    async with sem:
        return await fetch(url, session)

async def fetch_pages(urls, session):
    tasks = []
    sem = asyncio.Semaphore(LIMIT)

    for u in urls:
        tasks.append(asyncio.ensure_future(bound_fetch(u, session, sem)))

    return await asyncio.gather(*tasks)

