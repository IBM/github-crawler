#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import sys
import utils.github_utils as gh
from utils.cloudant_utils import cloudant_db as db, save_doc

def main(args):

    try:
        limit = int(args[0])
    except:
        limit = 50000

    try:
        skip = int(args[1])
    except:
        skip = 0

    try:
        sort = "asc" if args[2]=="asc" else "desc"
    except:
        sort = "desc"

    print(limit, skip, sort)

    repos = db.get_query_result({
                "type":"Repo",
                # "topics": { "$elemMatch": { "$in": ["machine-learning", "deep-learning" ] } },
                #"contributions": { "$exists": False }
            },["_id", "contributors_id"], limit=limit, skip=skip, raw_result=True, sort=[{'pushed_at': sort}] )["docs"]

    print("repos", len(repos))

    users = [ r["_id"] for r in db.get_query_result({"type":"User"},["_id"]) ]
    print("users", len(users))

    # crawl the repos
    for repo in repos:
        print("\n", repo["_id"])
        for c in repo["contributors_id"]:
            print(c)
            if c not in users:
                gh.get_user(c, overwrite=False, details=False)
                users.append(c)

if __name__ == "__main__":
    main(sys.argv[1:])
