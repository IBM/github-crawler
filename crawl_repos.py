#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import sys
import utils.github_utils as gh
from utils.cloudant_utils import cloudant_db as db
from utils import ISO_FORMAT, ISO_SHORT_FORMAT, format_date_utc_iso

def process(topic, stars, size):
    print(40*"+", topic)
    last_star = stars
    res_items = PAGE_SIZE
    while res_items == PAGE_SIZE:
        query = "topic:{} stars:>={} size:{} archived:false mirror:false".format(topic, last_star, size)
        print(query)
        res_items = 0
        repos_search_result = gh.get_client("search").search_repositories(query, number=PAGE_SIZE, sort="stars", order="asc")
        for r in repos_search_result:
            res_items += 1
            last_star = max(last_star, r.stargazers_count)
            print(res_items, topic, r.stargazers_count, last_star, r.full_name)
            if r.full_name not in repos:
                print(50*":", r.full_name )
                gh.extract_metadata(r.repository, overwrite=False, get_users=False, contributors_only=True)

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

    PAGE_SIZE = 500
    try:
        stars = int(sys.argv[2])
    except:
        stars = 0

    try:
        sizes = sys.argv[3].split(",") #[ "1..100", "101..200", "201..500", "501..1000", "1001..2000", "2001..5000", "5001..10000", ">10000"]
    except:
        sizes =  [">0"]

    for topic in topics:
        for size in sizes:
            process(topic, stars, size)

    # topics = [ "ai", "artificial-intelligence", "nn", "neural-network", "ml", "machine-learning", "dl", "deep-learning" ]
