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
    }, ["_id", "releases", "stars", "watchers", "commits_count", "forks_count", "issues_count"], limit=1, raw_result=True)["docs"]]

    releases = [{"repo": r["_id"], "release_tag": re["tag"], "release_date": re["published_at"],
                 "stars": 0, "watchers":0, "forks":0, "commits": 0, "issues":0} for r in repos for re in r["releases"] ][::-1]
    # print_json(releases)

    total_stars, stars_post_cutoff, stars_pre_cutoff = getStars(repos[0])
    total_watchers, watchers_post_cutoff, watchers_pre_cutoff = getWatchers(repos[0])
    total_forks, forks_post_cutoff, forks_pre_cutoff = getForks(repos[0])
    total_commits, commits_post_cutoff, commits_pre_cutoff = getCommits(repos[0])
    total_issues, issues_post_cutoff, issues_pre_cutoff = getIssues(repos[0])

    for release in releases:
        updateRelease(release, stars_post_cutoff, stars_pre_cutoff,  "stars")
        updateRelease(release, watchers_post_cutoff, watchers_pre_cutoff,  "watchers")
        updateRelease(release, forks_post_cutoff, forks_pre_cutoff,  "forks")
        updateRelease(release, commits_post_cutoff, commits_pre_cutoff,  "commits")
        updateRelease(release, issues_post_cutoff, issues_pre_cutoff,  "issues")
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
    # print("\n total watchers after cutoff (2020-01-01) :", len(watchers_post_cutoff))
    # print(total_watchers, watchers_pre_cutoff, watchers_post_cutoff[0])
    return total_watchers, watchers_post_cutoff, watchers_pre_cutoff


def getForks(repo):
    forks_post_cutoff = list(db[repo["_id"] + "/forks"]["forks"].values())[::-1]
    total_forks = repo["forks_count"]
    forks_pre_cutoff = total_forks - len(forks_post_cutoff)
    # print_json(forks_post_cutoff)
    # print("\n total forks after cutoff (2020-01-01) :", len(forks_post_cutoff))
    # print(total_forks, forks_pre_cutoff, forks_post_cutoff[0])
    return total_forks, forks_post_cutoff, forks_pre_cutoff


def getCommits(repo):
    commits_post_cutoff = list(c['date'] for c in db[repo["_id"] + "/commits"]["events"])[::-1]
    total_commits = repo["commits_count"]
    commits_pre_cutoff = total_commits - len(commits_post_cutoff)
    # print_json(len(commits_post_cutoff))
    # print("\n total stars after cutoff (2020-01-01) :", len(commits_post_cutoff))
    # print(total_commits, commits_pre_cutoff, commits_post_cutoff[0] if len(commits_post_cutoff) != 0 else None)
    return total_commits, commits_post_cutoff, commits_pre_cutoff


def getIssues(repo):
    issues_post_cutoff = list(i['createdAt'] for i in db[repo["_id"] + "/issues"]["events"])[::-1]
    total_issues = repo["issues_count"]
    issues_pre_cutoff = total_issues - len(issues_post_cutoff)
    # print_json(len(issues_post_cutoff))
    # print("\n total issues after cutoff (2020-01-01) :", len(issues_post_cutoff))
    # print(total_issues, issues_pre_cutoff, issues_post_cutoff[0] if len(issues_post_cutoff) != 0 else None)
    return total_issues, issues_post_cutoff, issues_pre_cutoff


if __name__ == "__main__":
    main(sys.argv[1:])