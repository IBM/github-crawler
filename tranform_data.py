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
    }, fields, limit=1, raw_result=True)["docs"]]

    releases = [{"repo": r["_id"], "release_tag": re["tag"], "release_date": re["published_at"], "downloads": re["download"],
                 "stars": 0, "watchers":0, "forks":0, "commits": 0, "issues":0} for r in repos for re in r["releases"] ][::-1]
    # print_json(releases)

    for repo in repos:
        total_stars, stars_post_cutoff, stars_pre_cutoff = getStars(repo)
        total_watchers, watchers_post_cutoff, watchers_pre_cutoff = getWatchers(repo)
        total_forks, forks_post_cutoff, forks_pre_cutoff = getForks(repo)
        total_commits, commits_post_cutoff, commits_pre_cutoff = getCommits(repo)
        total_issues, issues_post_cutoff, issues_pre_cutoff, issues = getIssues(repo)
        readme_dict = getReadmeContent(repo)
        contributor_statistics = repo["contributor_statistics"]

        #Tested for issues, stars, forks
        for i in range(len(releases)):
            # print("Repo:%s \t Tag: %s" % (repo['_id'],  releases[i]['release_tag']))
            r = releases[i]
            curr_release = r["release_date"]
            if curr_release < "2020-01-01":
                continue
            next_release = date.today() if i+1 == len(releases) else releases[i+1]["release_date"]
            prev_release = "2018-01-01" if i == 0 else releases[i-1]["release_date"]
            # print(i, prev_release, curr_release, next_release)

            updateRelease(curr_release,  next_release, r, stars_post_cutoff, stars_pre_cutoff,  "stars")
            updateRelease(curr_release,  next_release, r, watchers_post_cutoff, watchers_pre_cutoff,  "watchers")
            updateRelease(curr_release,  next_release, r, forks_post_cutoff, forks_pre_cutoff,  "forks")

            updateRelease(prev_release, curr_release, r, commits_post_cutoff, commits_pre_cutoff,  "commits")
            updateRelease(prev_release, curr_release, r, issues_post_cutoff, issues_pre_cutoff,  "issues", additional_dict=issues)
            r['readme'] = readme_dict[r['release_tag']]['text']
            r['readme_size'] = readme_dict[r['release_tag']]['byteSize']

            addContributorsInRelease(r, contributor_statistics)
        print_json(releases)
        save_doc(repo["_id"] + "/release", {"type": "release", "releases": releases})


def addContributorsInRelease(release, contributor_statistics):
    a = 0
    d = 0
    c_total = 0
    contributors = []
    for c in contributor_statistics:
        author = c['author_id']
        total = c['total']
        for w in c['weeks']:
            if w['w'] < release['release_date']:
                a += w["a"]
                d += w["d"]
                c_total += w["c"]
                contributors.append(author)
    release["added"] = a
    release["deleted"] = d
    release["changed"] = c_total #todo confirm the field
    release["contributors"] = len(set(contributors)) #todo use contributors login name


def updateRelease(R1, R2, release, events, initial_count, field, additional_dict=None):
    count = 0

    # if there is additional keys like "closed" in additional_dict, we sum these fields as well in release
    closedCount = 0
    if additional_dict:
        events = list(i['createdAt'] for i in additional_dict)
    i = 0
    for e in events:
        if R1 < e < R2:
            count += 1
            if additional_dict and 'closed' in additional_dict[i] and additional_dict[i]['closed']:
                closedCount += 1
        if e > R2:
            break
        i += 1

    release[field] = count
    if additional_dict:
        release[field + 'Closed'] = closedCount


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
    issues_post_cutoff = list(i['createdAt'] for i in issues)[::-1]
    total_issues = repo["issues_count"]
    issues_pre_cutoff = total_issues - len(issues_post_cutoff)
    return total_issues, issues_post_cutoff, issues_pre_cutoff, issues


if __name__ == "__main__":
    main(sys.argv[1:])