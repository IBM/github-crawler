#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import sys
import utils.github_utils as gh
from utils.cloudant_utils import cloudant_db as db, save_doc


def main(args):

    try:
        events = args[0].split(",")
    except:
        events = ["commits", "issues", "forks", "watchers", "stars", "readme"]

    try:
        limit = int(args[1])
    except:
        limit = 10000

    try:
        skip = int(args[2])
    except:
        skip = 0
    try:
        sort = "asc" if args[3] == "asc" else "desc"
    except:
        sort = "desc"
    
    try:
        since = args[4]
    except:
        since = "2020-01-01"

    if "commits" in events:
        get_commits_events(limit, skip, sort, cutoff=since)
    if "issues" in events:
        get_issues_events(limit, skip, sort, cutoff=since)
    if "forks" in events:
        get_forks_events(limit, skip, sort, cutoff=since)
    if "watchers" in events:
        get_watchers_events(limit, skip, sort, cutoff=since)
    if "readme" in events:
        get_README_history(limit, skip, sort, cutoff=since)
    if "stars" in events:
        get_stars_events(limit, skip, sort, cutoff=since)


def get_commits_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "releases.0": {"$exists": True},
        "commits_events": {"$exists": False},
        "commits_count": {"$gt": 0}
    }, ["_id", "commits_count"], limit=limit, skip=skip, raw_result=True, sort=[{'commits_count': sort}])["docs"]]

    print("repos", len(repos))
    for repo in repos:
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
        "releases.0": {"$exists": True},
        "issues_events": {"$exists": False},
        "issues_count": {"$gt": 0}
    }, ["_id", "issues_count"], limit=limit, skip=skip, raw_result=True, sort=[{'issues_count': sort}])["docs"]]

    print("repos", len(repos))

    for repo in repos:
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


def get_forks_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "releases.0": {"$exists": True},
        "forks_events": {"$exists": False},
        "forks_count": {"$gt": 0}
    }, ["_id", "forks_count"], limit=limit, skip=skip, raw_result=True, sort=[{'forks_count': sort}])["docs"]]
    print("repos", len(repos))

    for repo in repos:
        repo_id = repo["_id"]
        try:
            forks = gh.get_fork_history(repo_id, cutoff)
            forks = [] if forks is None else forks
            save_doc(repo_id + "/forks", {"type": "RepoForks", "repo_id": repo_id, "forks": forks})
            save_doc(repo_id, {
                "forks_events_id": repo_id + "/forks",
                "forks_events": len(forks)})
        except Exception as e:
            print(str(e))
            pass


def get_watchers_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "releases.0": {"$exists": True},
        "watchers_events": {"$exists": False},
        "watchers": {"$gt": 0}
    }, ["_id", "watchers"], limit=limit, skip=skip, raw_result=True, sort=[{'watchers': sort}])["docs"]]
    print("repos", len(repos))

    for repo in repos:
        repo_id = repo["_id"]
        try:
            watchers = gh.get_watcher_events(repo_id, cutoff)
            watchers = [] if watchers is None else watchers
            save_doc(repo_id + "/watchers", {"type": "RepoWatchers", "repo_id": repo_id, "watchers": watchers})
            save_doc(repo_id, {
                "watchers_events_id": repo_id + "/watchers",
                "watchers_events": len(watchers)})
        except Exception as e:
            print(str(e))
            pass


def get_README_history(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "readme_events": {"$exists": False},
        "releases.0": {"$exists": True},
    }, ["_id", "releases"], limit=limit, skip=skip, raw_result=True)["docs"]]

    print("repos", len(repos))
    i = 1
    for repo in repos:
        print("\n",  i, ". ", repo["_id"])
        i += 1
        repo_id = repo["_id"]
        releases = repo['releases']
        try:
            readme = gh.get_README_history(repo_id, releases, cutoff)
            readme = [] if readme is None else readme
            save_doc(repo_id + "/readme", {"type": "RepoReadme", "repo_id": repo_id, "readme": readme})
            save_doc(repo_id, {
                "readme_events_id": repo_id + "/readme",
                "readme_events": len(readme)})
        except Exception as e:
            print("---------------->", repo["_id"])
            print(str(e))
            pass


def get_stars_events(limit, skip, sort, cutoff):
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "releases.0": {"$exists": True},
        "stargazers_events": {"$exists": False},
        "stars": {"$gt": 0}
    }, ["_id", "stars"], limit=limit, skip=skip, raw_result=True, sort=[{'stars': sort}])["docs"]]
    print("repos", len(repos))

    for repo in repos:
        repo_id = repo["_id"]
        try:
            stargazers = gh.get_starred_events(repo_id, cutoff)
            stargazers = [] if stargazers is None else stargazers
            save_doc(repo_id + "/stargazers", {"type": "RepoStargazers", "repo_id": repo_id, "stargazers": stargazers})
            save_doc(repo_id, {
                "stargazers_events_id": repo_id + "/stargazers",
                "stargazers_events": len(stargazers)})
        except Exception as e:
            print(str(e))
            pass


if __name__ == "__main__":
    main(sys.argv[1:])