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

    repos = [ r for r in db.get_query_result({
                "type":"Repo",
                "stargazers_events": { "$exists": False },
                "stars": { "$gt": 0 }
            },["_id", "stars"], limit=limit, skip=skip, raw_result=True, sort=[{'stars': sort}] )["docs"] ]

    print("repos", len(repos))

    for repo in repos:
        print("\n", repo)
        repo_id = repo["_id"]
        try:
            starred_events = gh.get_starred_events(repo_id, "2019-01-01")
            starred_events = [] if starred_events is None else starred_events
            save_doc(repo_id+"/stargazers", {"type": "RepoStargazers", "repo_id": repo_id, "events": starred_events })
            save_doc(repo_id, {
                "starred_events": None,
                "stargazers_events_id": repo_id+"/stargazers",
                "stargazers_events": len(starred_events),
                "stars": gh.get_repo_by_fullname(repo_id).stargazers_count })
        except Exception as e:
            print(str(e))
            pass

if __name__ == "__main__":
    main(sys.argv[1:])
