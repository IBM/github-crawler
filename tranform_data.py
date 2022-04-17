#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import sys
from utils import print_json
from utils.cloudant_utils import cloudant_db as db, save_doc
from datetime import date


def main(args):
    fields = ["_id", "releases", "stars", "watchers", "commits_count", "forks_count", "issues_count", "contributor_statistics"]
    repos = [r for r in db.get_query_result({
        "type": "Repo",
        "releases.0": {"$exists": True},
        # "release_events": {"$exists": False},
    }, fields, limit=100000, raw_result=True)["docs"]]

    releases = [{"repo": r["_id"], "release_tag": re["tag"], "release_date": re["published_at"], "downloads": re["download"],
                 "stars": 0, "watchers":0, "forks":0, "commits": 0, "issues":0} for r in repos for re in r["releases"] ][::-1]
    print("Total Releases for all repos: ", len(releases))

    i = 0
    for repo in repos:
        try:
            repo_releases = [r for r in releases if r['repo'] == repo['_id']]
            print("\n ", i, ". ", repo["_id"], " :: ",  len(repo_releases))
            _, s_events, s_initial_count = getStars(repo)
            _, w_events, w_initial_count = getWatchers(repo)
            _, f_events, f_initial_count = getForks(repo)
            _, c_events, c_initial_count = getCommits(repo)
            _, i_events, i_initial_count, closed_issues = getIssues(repo)

            updateReleases(repo_releases, "stars", s_events, s_initial_count)
            updateReleases(repo_releases, "watchers", w_events, w_initial_count)
            updateReleases(repo_releases, "forks", f_events, f_initial_count)
            updateReleases(repo_releases, "commits", c_events, c_initial_count)
            updateReleases(repo_releases, "issues", i_events, i_initial_count)
            updateReleases(repo_releases, "closedIssues", closed_issues, 0)  # todo get initial closedIssues count

            addReadme(repo_releases, repo)
            # addContributorsInRelease(repo_releases, repo)

            # print(repo_releases)
            save_doc(repo["_id"] + "/release", {"type": "release", "releases": repo_releases})
            save_doc(repo["_id"], {
                "release_events_id": repo["_id"] + "/release",
                "release_events": len(repo_releases)})
            i += 1

        except Exception as e:
            print(str(e))
            pass


def addReadme(releases, repo):
    readme_dict = getReadmeContent(repo)
    for r in releases:
        r['readme'] = readme_dict[r['release_tag']]['text']
        r['readme_size'] = readme_dict[r['release_tag']]['byteSize']


def addContributorsInRelease(releases, repo):
    contributor_statistics = repo["contributor_statistics"]
    for r in releases:
        a = 0
        d = 0
        c_total = 0
        contributors = []
        for c in contributor_statistics:
            author = c['author_id']
            total = c['total']
            for w in c['weeks']:
                if w['w'] < r['release_date']:
                    a += w["a"]
                    d += w["d"]
                    c_total += w["c"]
                    contributors.append(author)
        r["added"] = a
        r["deleted"] = d
        r["changed"] = c_total #todo confirm the field
        r["contributors"] = len(set(contributors)) #todo use contributors login name


# Demostration of algorithm: makes a range query on releases,
# i.e if an event (e1) falls in between R1 and R2 , then it belongs to R1 release

# Example 1:
#               *------+---+---*----+---------*--+--
# Release:      R1             R2             R3
# Events:            e1 e2       e3            e4

# Example 2:
#               *--------------*--------------*-----+
# Release:      R1             R2             R3
# Events:                                           e5

# Solution:
# Example 1:  R1={e1,e2}, R2={e3},  R3={e4}
# Example 2:  R1={}, R2={},  R3={e5}
def updateReleases(releases, field, events, initial_count, agg=True):
    i = 0
    c = releases[i]['release_date']
    n = str(date.today()) if i+1 == len(releases) else releases[i+1]['release_date']
    count = 0
    for e in events:
        if e < c:
            initial_count += 1
            continue
        while e > n:
            c = n
            n = str(date.today()) if i+2 == len(releases) else releases[i+2]['release_date']
            releases[i]['total_'+field] = initial_count + count if agg else count
            # releases[i]["initial_"+field] = initial_count
            releases[i][field] = count
            i += 1
            initial_count = initial_count + count
            count = 0

        if c <= e < n:
            count += 1
    while i < len(releases):
        releases[i]["total_"+field] = initial_count + count if agg else count
        # releases[i]["initial_"+field] = initial_count
        releases[i][field] = count
        i += 1
        initial_count = initial_count + count
        count = 0


def getReadmeContent(repo):
    read_me_content = db[repo["_id"] + "/readme"]["readme"]['content']
    return read_me_content


def getStars(repo):
    stars_post_cutoff = list(db[repo["_id"] + "/stargazers"]["stargazers"].values())[::-1]
    total_stars = repo["stars"]
    stars_pre_cutoff = total_stars - len(stars_post_cutoff)
    return total_stars, stars_post_cutoff, stars_pre_cutoff


def getWatchers(repo):
    watchers_post_cutoff = list(db[repo["_id"] + "/watchers"]["watchers"].values())[::-1]
    total_watchers = repo["watchers"]
    watchers_pre_cutoff = total_watchers - len(watchers_post_cutoff)
    return total_watchers, watchers_post_cutoff, watchers_pre_cutoff


def getForks(repo):
    forks_post_cutoff = list(db[repo["_id"] + "/forks"]["forks"].values())[::-1]
    total_forks = repo["forks_count"]
    forks_pre_cutoff = total_forks - len(forks_post_cutoff)
    return total_forks, forks_post_cutoff, forks_pre_cutoff


def getCommits(repo):
    commits_post_cutoff = list(c['date'] for c in db[repo["_id"] + "/commits"]["events"])[::-1]
    total_commits = repo["commits_count"]
    commits_pre_cutoff = total_commits - len(commits_post_cutoff)
    return total_commits, commits_post_cutoff, commits_pre_cutoff


def getIssues(repo):
    issues = db[repo["_id"] + "/issues"]["events"]
    issues = sorted(issues, key=lambda i: i['createdAt'])
    issues_post_cutoff = list(i['createdAt'] for i in issues)
    total_issues = repo["issues_count"]
    issues_pre_cutoff = total_issues - len(issues_post_cutoff)

    closed_issues = [i['closedAt'] for i in issues]
    closed_issues = sorted(list(filter(None, closed_issues)))

    return total_issues, issues_post_cutoff, issues_pre_cutoff, closed_issues


if __name__ == "__main__":
    main(sys.argv[1:])