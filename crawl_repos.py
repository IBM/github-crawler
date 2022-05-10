#
# Copyright 2022- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
from functools import total_ordering
import sys, json
import utils.github_utils as gh
from utils.cloudant_utils import cloudant_db as db
from utils import ISO_FORMAT, ISO_SHORT_FORMAT, format_date_utc_iso

PAGE_SIZE = 500

def process(topic, stars, size):
    print(40*"+", topic)
    last_star = stars
    res_items = PAGE_SIZE
    total = 0
    while res_items == PAGE_SIZE:
        query = "topic:{} stars:>={} size:{} archived:false mirror:false".format(topic, last_star, size)
        print(20*"\n", query)
        res_items = 0
        repos_search_result = gh.get_client("search").search_repositories(query, number=PAGE_SIZE, sort="stars", order="asc")
        for r in repos_search_result:
            res_items += 1
            total += 1
            last_star = max(last_star, r.stargazers_count)
            print(size, stars, total, res_items, topic, r.stargazers_count, last_star, r.full_name)
            if r.full_name not in repos:
                print(50*":", r.full_name )
                gh.extract_metadata(r.repository, overwrite=False, get_users=False, contributors_only=False)
    return total

if __name__ == "__main__":
    try:
        topics = sys.argv[1].split(",")
    except:
        print("missing topic name")
        exit(0)

    results = db.get_view_result(
                    '_design/types',
                    "types",
                    key="Repo",
                    reduce=False,
                    descending=False,
                    page_size=100000,
                    skip=0)
    repos = [ r['id'] for r in results ]
    print(repos)

    try:
        stars = int(sys.argv[2])
    except:
        stars = 0

    try:
        sizes = sys.argv[3].split(",")
    except:
        sizes = []
        for n in range(1,50,10): 
            sizes.append(str(n) +".."+ str(n+10-1) )
        for n in range(51,1000,50): 
            sizes.append(str(n) +".."+ str(n+50-1) )
        for n in range(1001,10000,500): 
            sizes.append(str(n) +".."+ str(n+500-1) )
        for n in range(10001,100000,5000): 
            sizes.append(str(n) +".."+ str(n+5000-1) )
        sizes.append("100001..200000")
        sizes.append(">200000")

    print(sizes)

    total  = 0
    for topic in topics:
        for size in sizes:
            total += process(topic, stars, size)
            # total += gh.repo_search_count(topic, stars, size)

    print(total)
    # topics = [ "ai", "artificial-intelligence", "nn", "neural-network", "ml", "machine-learning", "dl", "deep-learning" ]
