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
        limit = 1000

    try:
        skip = int(args[1])
    except:
        skip = 0

    try:
        sort = "asc" if args[2]=="asc" else "desc"
    except:
        sort = "desc"

    repos = [ r["_id"] for r in db.get_query_result({
                "type":"Repo",
                # "topics": { "$elemMatch": { "$in": ["machine-learning", "deep-learning" ] } },
                "starred_events": {"$or": [ { "$exists": False }, { "$eq": None } ]},
                # "stars": { "$gt": 5 }
            },["_id"], limit=limit, skip=skip, raw_result=True, sort=[{'stars': sort}] )["docs"] ]

    print("repos", len(repos))

    for repo_id in repos:
        print("\n", repo_id)
        try:
            starred_events = gh.get_starred_events(repo_id)
            if starred_events is not None:
                # print(len(starred_events))
                save_doc(repo_id, {"starred_events": starred_events, "stargazers_id": None })
        except Exception as e:
            print(str(e))
            pass

if __name__ == "__main__":
    main(sys.argv[1:])