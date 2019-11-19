import parse_spoj 
import argparse
import asyncio
import aiohttp
import client_async_sem
import csv
from urllib.parse import urljoin
from functools import reduce
from operator import itemgetter
from multiprocessing import Pool
import pdb


def merge_dicts(d1, d2):
    d = d1.copy()
    d.update(d2)
    return d


async def crawler(problem_name, pages):
    problem_rank_page = f"https://www.spoj.com/ranks/{problem_name}/"
    user_page = "https://spoj.com/users/"

    rank_urls = [urljoin(problem_rank_page, f"start={s}")\
                 for s in range(0, pages * 20, 20)]

    async with aiohttp.ClientSession() as s:
        rank_pages = await client_async_sem.fetch_pages(rank_urls, s)

    with Pool(3) as p:
        rank_results = p.map(parse_spoj.parse_rank_page, rank_pages)

    big_dict_of_results = reduce(merge_dicts, rank_results)
    simplified_results = list(big_dict_of_results.values())

    users = set(map(itemgetter(2), simplified_results))
    user_urls = [urljoin(user_page, u) for u in users]

    async with aiohttp.ClientSession() as s:
        user_pages = await client_async_sem.fetch_pages(user_urls, s)

    with Pool(3) as p:
        user_results = p.map(parse_spoj.parse_user_page, user_pages)

    big_dict_of_users = reduce(merge_dicts, user_results)

    simplified_results[0].insert(3, "institution")
    for x in simplified_results[1:]:
        user = x[2]
        if user in big_dict_of_users:
            x.insert(3, big_dict_of_users[user])

        else:
            x.insert(3, "n/a")

    with open(f"{problem_name}_results.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(simplified_results)


def main():
    loop = asyncio.get_event_loop()
    parser = argparse.ArgumentParser(description=\
        "Get ranks of solutions to a SPOJ problem")
    parser.add_argument("--problem", type=str, help="The code name of the problem")
    parser.add_argument("--pages", type=int, help="The number of pages of results to return")
    args = vars(parser.parse_args())
    loop.run_until_complete(crawler(args["problem"], args["pages"]))


if __name__ == "__main__":
    main()

