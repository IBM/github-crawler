#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import sys
from utils.cloudant_utils import cloudant_db as db
from utils import print_json


def main(args):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "releases.10": {"$exists": True}
    }, ["_id", "releases", "stars", "watchers"], limit=1, raw_result=True)["docs"]]

    releases = [{"repo": r["_id"], "release_tag": re["tag"], "release_date": re["published_at"], "stars": 0 } for r in repos for re in r["releases"] ][::-1]
    # print_json(releases)

    total_stars, stars_post_cutoff, stars_pre_cutoff = getStars(repos[0])
    total_watchers, watchers_post_cutoff, watchers_pre_cutoff = getWatchers(repos[0])

    for release in releases:
        updateRelease(release, stars_post_cutoff, stars_pre_cutoff,  "stars")
        updateRelease(release, watchers_post_cutoff, watchers_pre_cutoff,  "watchers")
    print_json(releases)


def updateRelease(release, events, initial_count, field):
    # events = []
    for e in events:
        if e < release["release_date"]:
            initial_count += 1
            # events.append(e)
        else:
            release[field] = initial_count
            # release["new_events"] = len(events)
            # release["events"] = events


def getStars(repo):
    stars_post_cutoff = list(db[repo["_id"] + "/stargazers"]["stargazers"].values())[::-1]
    total_stars = repo["stars"]
    stars_pre_cutoff = total_stars - len(stars_post_cutoff)
    # print_json(stars_post_cutoff)
    # print("\n total stars after cutoff (2020-01-01) :", len(stars_post_cutoff))
    # print(total_stars, watchers_pre_cutoff, stars_post_cutoff[0])
    return total_stars, stars_post_cutoff, stars_pre_cutoff


def getWatchers(repo):
    watchers_post_cutoff = list(db[repo["_id"] + "/watchers"]["watchers"].values())[::-1]
    print(repo)
    total_watchers = repo["watchers"]
    watchers_pre_cutoff = total_watchers - len(watchers_post_cutoff)
    # print_json(watchers_post_cutoff)
    # print("\n total stars after cutoff (2020-01-01) :", len(watchers_post_cutoff))
    # print(total_watchers, watchers_pre_cutoff, watchers_post_cutoff[0])
    return total_watchers, watchers_post_cutoff, watchers_pre_cutoff


if __name__ == "__main__":
    main(sys.argv[1:])