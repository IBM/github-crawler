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
        sort = "asc" if args[2] == "asc" else "desc"
    except:
        sort = "desc"

    get_commit_events(limit, skip, sort, cutoff="2019-01-01")
    get_issues_events(limit, skip, sort, cutoff="2019-01-01")
    get_fork_events(limit, skip, sort, cutoff="2019-01-01")
    get_watcher_events(limit, skip, sort, cutoff="2019-01-01")


def get_commit_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "commits_events": {"$exists": False},
        "commits_count": {"$gt": 0}
    }, ["_id", "commits_count"], limit=limit, skip=skip, raw_result=True, sort=[{'commits_count': sort}])["docs"]]

    print("repos", len(repos))

    for repo in repos:
        print("\n", repo)
        repo_id = repo["_id"]
        try:
            commits = gh.get_commit_history(repo_id, cutoff)
            commits = [] if commits is None else commits
            save_doc(repo_id + "/commits", {"type": "RepoCommits", "repo_id": repo_id, "events": commits})
            save_doc(repo_id, {
                "commits_events_id": repo_id + "/commits",
                "commits_events": len(commits)})
        except Exception as e:
            print(str(e))
            pass


def get_issues_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "issues_events": {"$exists": False},
        "issues_count": {"$gt": 0}
    }, ["_id", "issues_count"], limit=limit, skip=skip, raw_result=True, sort=[{'issues_count': sort}])["docs"]]

    print("repos", len(repos))

    for repo in repos:
        print("\n", repo)
        repo_id = repo["_id"]
        try:
            issues = gh.get_issues_history(repo_id, cutoff)
            issues = [] if issues is None else issues
            save_doc(repo_id + "/issues", {"type": "RepoIssues", "repo_id": repo_id, "events": issues})
            save_doc(repo_id, {
                "issues_events_id": repo_id + "/issues",
                "issues_events": len(issues)})
        except Exception as e:
            print(str(e))
            pass


def get_fork_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "forks_events": {"$exists": False},
        "forks_count": {"$gt": 0}
    }, ["_id", "forks_count"], limit=limit, skip=skip, raw_result=True, sort=[{'forks_count': sort}])["docs"]]

    print("repos", len(repos))

    for repo in repos:
        print("\n", repo)
        repo_id = repo["_id"]
        try:
            forks = gh.get_fork_history(repo_id, cutoff)
            print (forks)
            forks = [] if forks is None else forks
            save_doc(repo_id + "/forks", {"type": "RepoForks", "repo_id": repo_id, "forks": forks})
            save_doc(repo_id, {
                "forks_events_id": repo_id + "/forks",
                "forks_events": len(forks)})
        except Exception as e:
            print(str(e))
            pass


def get_watcher_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "watcher_events": {"$exists": False},
        "watchers": {"$gt": 0}
    }, ["_id", "watchers"], limit=limit, skip=skip, raw_result=True, sort=[{'watchers': sort}])["docs"]]

    print("repos", len(repos))

    for repo in repos:
        print("\n", repo)
        repo_id = repo["_id"]
        try:
            print("repo idddd>>>>>", repo_id)
            watchers = gh.get_watcher_events(repo_id, "2016-01-01")
            print (watchers)
            watchers = [] if watchers is None else watchers
            save_doc(repo_id + "/watchers", {"type": "RepoWatchers", "repo_id": repo_id, "watchers": watchers})
            save_doc(repo_id, {
                "watchers_events_id": repo_id + "/watchers",
                "watchers_events": len(watchers)})
        except Exception as e:
            print(str(e))
            pass


if __name__ == "__main__":
    main(sys.argv[1:])
