#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
from platform import version
import github3
import os, json, time
import markdown
import moment
import requests
from random import randrange
from .cloudant_utils import cloudant_db as db, save_doc
from . import ISO_FORMAT, format_date_utc_iso, ISO_SHORT_FORMAT, now_short

from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("GITHUB_API_URL", "https://api.github.com")
HTTP_API_URL = API_URL if "api" in API_URL else API_URL.replace("https://", "https://api.")
try:
    n = int(os.getenv("TOKENS"))
except:
    n = None
gh_tokens = os.getenv("GITHUB_TOKENS").split(",")[0:n]

# print(API_URL, gh_tokens)

def login(token):
    if "ibm" in API_URL:
        return github3.enterprise_login(url=API_URL, username=token.split(":")[0], password=token.split(":")[1])
    else:
        return github3.login(token=token)

gh_clients = []
for t in gh_tokens:
    c = login(t)
    try:
        c.meta()
        print(t, "valid")
        gh_clients.append(c)
    except Exception as e:
        gh_tokens.pop(gh_tokens.index(t))
        print(t, str(e))


def get_max_index_limit(resource="core"):
    limit = {"core": 200, "search": 5, "graphql": 100}
    try:
        # print([ gh.rate_limit() for gh in gh_clients ])
        gh_limits = [gh.rate_limit()['resources'][resource]["remaining"] for gh in gh_clients]
        max_value = max(gh_limits)
        if max_value < limit[resource]:
            seconds = (moment.unix(gh_clients[gh_limits.index(max_value)].rate_limit()['resources'][resource][
                                       "reset"]) - moment.now()).total_seconds()
            print("Sleeping " + str(seconds) + "s")
            time.sleep(seconds)
        max_index = gh_limits.index(max_value)
        # print(gh_limits, max_index, gh_clients[max_index].rate_limit()['resources'])
        # print([ gh.rate_limit()['resources']["core"] for gh in gh_clients ])
        return max_index
    except Exception as e:
        # print("Exception", str(e))
        return randrange(len(gh_clients))


def get_token(resource="core"):
    return gh_tokens[get_max_index_limit(resource)]

def get_authorization_header(resource="core"):
    t = gh_tokens[get_max_index_limit(resource)]
    if ":" in t:
        import base64
        return "Basic {}".format(base64.b64encode(t.encode()).decode())
    else:
        return "Bearer {}".format(t)

def get_rand_client():
    i = randrange(len(gh_clients))
    try:
        cli = gh_clients[i]
        for u in cli.search_users("type:org", number=1):
            continue
        cli.meta()
        # print([e.as_dict() for e in cli.emails()][0]["email"])
        # print(40*"-",i, gh_tokens[i].split(":")[0])
        return cli
    except github3.exceptions.ForbiddenError as e:
        print(i, gh_tokens[i].split(":")[0], "ForbiddenError")
        return get_rand_client()


def get_client(resource="core"):
    if "ibm" in API_URL:
        return get_rand_client()
    else:
        return gh_clients[get_max_index_limit(resource)]


def get_user(user_obj, overwrite=False, details=True, ignore_updated=True):
    try:
        login = user_obj.login
    except:
        login = user_obj

    today = moment.utcnow().zero
    if login in db and overwrite is False:  # and moment.date(cloudant_db[login]["updated_at"]) >= moment.date(user_obj.updated_at):
        print(login, "already in db")
        # try:
        #     if user_obj.type == "User":
        #         for o in get_client().user(login).organizations():
        #             get_user(o, overwrite)
        # except:
        #     pass
        return db[login]
    elif login in db and ignore_updated and moment.date(db[login].get("crawled_updated_at", "2000")).zero == today:
        print(20 * "*", login, "already updated today")
        return db[login]
    else:
        if type(user_obj) != github3.users.User:
            try:
                user_obj = user_obj.refresh()
            except Exception as e:
                print(login, type(user_obj), str(e))
                user_obj = get_client().user(login)
        full_user = user_obj.as_dict()
        result = {k: full_user.get(k, None) for k in (
        'login', 'id', 'type', 'blog', 'company', 'name', 'location', 'bio', 'email', "twitter_username",
        "public_repos", "public_gists", "followers", "following", "created_at", "updated_at", "suspended_at")}
        result["_id"] = result["login"]
        result["created_at"] = format_date_utc_iso(result["created_at"])
        result["updated_at"] = format_date_utc_iso(result["updated_at"])
        result["suspended_at"] = format_date_utc_iso(result["suspended_at"]) if "suspended_at" in result and result[
            "suspended_at"] is not None else None
        del result["login"]

        if details is True:
            # result["repos"] = get_user_repos(login, result["type"])
            if result["type"] == "User":
                result["orgs"] = []
                print(login, "get organizations")
                for o in get_client().user(login).organizations():
                    result["orgs"].append(o.login)
                    # get_user(o, overwrite)
                print(login, "get commits")
                commits = search_commits(login, from_date=2020)
                result["commits_2020"] = commits
                result["commits_count_2020"] = len(commits)
                result["commits_repos_2020"] = list(set([c['repo'] for c in commits if 'repo' in c]))

                print(login, "get all commits")
                try:
                    q = "sort:committer-date-desc committer:{} author:{}".format(login, login)
                    print(q)
                    all_commits = get_client("search").search_commits(q, number=1)
                    for i in all_commits:
                        continue
                    print("all_commits", all_commits.total_count)
                    result["commits_count_all"] = all_commits.total_count
                except Exception as e:
                    print(type(e), str(e))
                    result["commits_count_all"] = 0

                for issue_pr in ["issue", "pr"]:
                    for verb in ["involves"]:  # , "author", "assignee", "mentions", "commenter"]:
                        result["{}s_{}".format(issue_pr, verb)] = get_user_search_issues(login, verb, issue_pr)

            print(login, "get repos")
            result["repos"] = [r.full_name for r in get_client().repositories_by(login)]

        return save_doc(login, result)


def get_repos(user_login, type="org"):
    print(user_login, type, 40 * "*")
    if type == "org":
        if user_login in db and "repos" in db[user_login]:
            return db[user_login]["repos"]
        else:
            org = get_user(user_login)
            return org["repos"]
    else:
        return [r.full_name for r in get_client().repositories_by(user_login)]


def get_user_repos(user_login, type):
    print(15 * "-", "get repos", user_login, type)
    repos = get_repos(user_login, "user")
    if type == "User":
        for r in get_client().user(user_login).organizations():
            repos += get_repos(r.login, "org")
    # repos.sort()
    return repos


def save_readme_file(file_path, repo):
    print(15 * "-", "get readme")
    readme_html = ""
    try:
        readme = repo.readme()
        if readme is None or len(str(readme)) == 0:
            print("No readme available!\n\n")
        else:
            readme_html = markdown.markdown(readme.decoded.decode('utf-8'))
    except github3.exceptions.NotFoundError as nfe:
        print("readme not found!!!\n\n")
        pass
    except Exception as e:
        print("error parsing readme!!!\n", str(e))
        pass
    with open(file_path, 'w') as outfile:
        outfile.write("<html>\n")
        outfile.write("<head>\n")
        outfile.write("<meta content=\"text/html; charset=UTF-8\" http-equiv=\"Content-Type\"/>\n")
        outfile.write("<title>")
        outfile.write(repo.description.strip() if repo.description is not None else repo.full_name)
        outfile.write("</title>\n")
        outfile.write("</head>\n")
        outfile.write("<body>\n")
        outfile.write(readme_html)
        outfile.write("\n</body>\n")
        outfile.write("</html>\n")


def file_name(repo_name, ext="json"):
    return repo_name.replace("/", "_").replace(".", "_") + "." + ext


def get_community_profile(full_name):
    print(full_name, "get community profile")
    try:
        return requests.request("GET",
                                API_URL + "/repos/" + full_name + "/community/profile",
                                headers={"Accept": "application/vnd.github.black-panther-preview+json"},
                                auth=('', get_token())).json()
    except:
        return {}


def get_commits(full_name):
    print(full_name, "get commits total")
    token = get_token("search")
    user = ""
    if ":" in token:
        p = token.split(":")
        user = p[0]
        token = p[1]

    url = ("https://api.github.ibm.com" if "ibm" in API_URL else API_URL) + "/search/commits"
    querystring = {"q": "repo:{} committer-date:>2000".format(full_name), "per_page": "1"}
    payload = ""
    headers = {
        "Accept": "application/vnd.github.cloak-preview"
    }
    return requests.request("GET", url, data=payload, headers=headers, params=querystring, auth=(user, token)).json()[
        "total_count"]


def extract_metadata(repo, current_commits=[], overwrite=False, get_users=True, user_details=False,
                     contributors_only=False):
    # repo_name = repo if type(repo) is str else repo.full_name
    # if repo_name in db and "crawled_updated_at" in db[repo_name] and db[repo_name]["crawled_updated_at"] > now_short():
    #     print(repo_name, "already updated today")
    #     return db[repo_name]

    if type(repo) is str:
        try:
            # if overwrite is True and repo in db and db[repo]["public"] is False:
            #     doc = db[repo]
            #     doc.delete()
            repo = get_repo_by_fullname(repo)
        except:
            # save repo not found or private
            metadata = {
                "_id": repo,
                "type": "Repo",
                "owner_id": repo.split("/")[0],
                "name": repo.split("/")[1],
                "description": "not found or private",
                "public": False
            }
            print("REPO NOT FOUND", repo)
            return save_doc(repo, metadata)

    if repo.full_name in db and overwrite is False:
        print(20 * "*", repo.full_name, "already in db")
        print(15 * "-", "get owner")
        get_user(repo.owner, overwrite, details=user_details)
        if get_users:
            print(15 * "-", "get contributors")
            for c in repo.contributors():
                get_user(c, overwrite, details=user_details)
            if not contributors_only:
                print(15 * "-", "get subscribers")
                for s in repo.subscribers():
                    get_user(s, overwrite, details=user_details)
                print(15 * "-", "get stargazers")
                for s in repo.stargazers():
                    get_user(s, overwrite, details=user_details)
        return db[repo.full_name]

    # extract metadata
    if isinstance(repo, github3.repos.repo.ShortRepository):
        print(repo.full_name, "refresh repo")
        try:
            repo = repo.refresh()
        except github3.exceptions.ForbiddenError:
            repo = get_repo_by_fullname(repo.full_name)

    firstSunday = moment.now().replace(weekday=7).subtract(weeks=53).zero
    print(repo.full_name, "get metadata")
    metadata = {
        "_id": repo.full_name,
        "type": "Repo",
        "owner_id": repo.owner.login,
        "name": repo.name,
        "description": repo.description,
        "homepage": repo.homepage,
        # "url": None,
        "created_at": format_date_utc_iso(repo.created_at),
        "updated_at": format_date_utc_iso(repo.updated_at),
        "pushed_at": format_date_utc_iso(repo.pushed_at),
        "forks_count": repo.forks_count,
        "fork": repo.fork,
        "network_count": repo.network_count,
        "size": repo.size,
        # "source": repo.source,
        "open_issues": repo.open_issues_count,
        # "archived": repo.archived,
        "disabled": repo.disabled,
        # "mirror_url": repo.mirror_url,
        # "public": True,
        "has_downloads": repo.has_downloads,
        "has_issues": repo.has_issues,
        "has_pages": repo.has_pages,
        "has_wiki": repo.has_wiki,
        # "branches": [ b.as_dict() for b in repo.branches() ],
        # "code_frequency": repo.code_frequency().as_dict(),
        # "commit_activity": repo.commit_activity().as_dict(),
        # "labels": [ c.as_dict() for c in repo.labels() ], # related to issues
        # "hooks": [ c.as_dict() for c in repo.hooks() ],
        # "refs": [ c.as_dict() for c in repo.refs() ],
        # "tags": [ c.as_dict() for c in repo.tags() ],
        # "events": [ c.as_dict() for c in repo.events() ],
        # deployments
        # contents = dict(repo.directory_contents('path/to/dir/'))
        # forks
        # milestones
        # pages
        # projects
    }

    metadata["readme"] = False
    print(repo.full_name, "get README")
    try:
        readme = repo.readme().decoded.decode('utf-8')
        if readme is None or len(str(readme)) == 0:
            print(repo.full_name, "No readme available!\n\n")
        else:
            metadata["readme"] = True
    except github3.exceptions.NotFoundError as nfe:
        print(repo.full_name, "readme not found!!!\n\n")
        pass
    except Exception as e:
        print(repo.full_name, "error parsing readme!!!\n", str(e))
        pass

    # releases
    metadata["download_count"] = 0
    print(repo.full_name, "get download counts")
    try:
        releases = repo.releases()
        downloads = {}
        for r in releases:
            if r.tag_name not in downloads:
                downloads[r.tag_name] = 0
            for a in r.assets():
                downloads[r.tag_name] += a.download_count
        metadata["releases"] = [
            {"tag": r.tag_name,
             "published_at": format_date_utc_iso(r.published_at),
             "download": downloads[r.tag_name]
             }
            for r in releases if r.name is not None or r.name != ""]
        # downloads = [ a.download_count for r in releases for a in r.assets() ]
        metadata["download_count"] = sum(downloads.values())
    except Exception as e:
        print(repo.full_name, "error getting releases!!\n", str(e))
        pass

    try:
        my_repo_doc = save_doc(repo.full_name, metadata)
        try:
            if metadata["readme"]:
                print(repo.full_name, "put README attachment")
                my_repo_doc.put_attachment("README.md", "text/markdown; charset=utf-8", readme.encode('utf-8'))
        except Exception as e:
            print("ERROR attaching readme", repo.full_name, str(e))
    except Exception as e:
        print(metadata)
        print("ERROR SAVING REPO", repo.full_name, str(e))

    try:
        con_stat = []
        for cs in repo.contributor_statistics():
            aconrtib = cs.as_dict()
            weeks = []
            for week in aconrtib['weeks']:
                if week['a'] != 0 | week['d'] != 0 | week['c'] != 0:
                    week.update(w=moment.unix(week['w']).strftime(ISO_SHORT_FORMAT))
                    weeks.append(week)
            aconrtib.update(weeks=weeks)
            aconrtib["author_id"] = aconrtib['author']['login']
            del aconrtib['author']
            con_stat.append(aconrtib)
        metadata["contributor_statistics"] = con_stat
    except Exception as e:
        print(repo.full_name, "error parsing contributor_statistics!!!\n", str(e))
        pass

    try:
        metadata.update(license=repo.license().license.name)
    except Exception as e:
        print(repo.full_name, "error getting license!!!\n", str(e))
        pass

    # languages
    metadata["main_language"] = repo.language
    try:
        metadata["all_languages"] = [l[0] for l in repo.languages() if l]
    except Exception as e:
        print(repo.full_name, "error getting languages!!!\n", str(e))
        pass

    # topics
    try:
        metadata["topics"] = repo.topics().names
    except Exception as e:
        print(repo.full_name, "error getting topics!!!\n", str(e))
        pass

    # weekly_commit_count
    try:
        metadata["commits_weekly"] = [{"week": firstSunday.add(weeks=1).strftime(ISO_SHORT_FORMAT), "value": c} for c in
                                      repo.weekly_commit_count()["all"]]
    except Exception as e:
        print(repo.full_name, "error getting commits!!!\n", str(e))
        pass
    # merge commits!
    if len(current_commits) > 0 and len(metadata["commits"]) > 0:
        print(repo.full_name, "merge commits")
        first_index = [i for i, x in enumerate(current_commits) if x["week"] == metadata["commits_weekly"][0]["week"]][
            0]
        metadata["commits_weekly"] = current_commits[0:first_index] + metadata["commits_weekly"]

    # contributors
    try:
        print(repo.full_name, "get contributors")
        metadata["contributions"] = {c.login: c.contributions for c in repo.contributors()}
        metadata["contributors_id"] = list(metadata["contributions"].keys())
        metadata["contributions_total"] = sum(metadata["contributions"].values())
    except Exception as e:
        print(repo.full_name, "error parsing contributors!!!\n", str(e))
        pass

    # subscribers
    metadata["watchers"] = repo.watchers_count  # The number of people watching this repository.
    metadata["subscribers_count"] = repo.subscribers_count  # The number of people watching (or who have subscribed to notifications about) this repository.
    try:
        print(repo.full_name, "get subscribers")
        subscribers = repo.subscribers()
        metadata["subscribers_id"] = []
        for s in subscribers:
            metadata["subscribers_id"].append(s.login)
    except Exception as e:
        print(repo.full_name, "error parsing subscribers!!!\n", str(e))
        pass

    # stargazers
    metadata["stars"] = repo.stargazers_count  # The number of people who have starred this repository.
    try:
        print(repo.full_name, "get stargazers")
        stargazers = repo.stargazers()
        metadata["stargazers_id"] = []
        for s in stargazers:
            metadata["stargazers_id"].append(s.login)
    except Exception as e:
        print(repo.full_name, "error parsing stargazers!!!\n", str(e))
        pass

    # assignees
    # try:
    #     print(repo.full_name, "get assignees")
    #     assignees = repo.assignees()
    #     metadata["assignees_count"] = len(list(assignees))
    #     metadata["assignees"] = [ c.login for c in assignees ]
    #     metadata["assignees_details"] = [ get_user(c) for c in assignees ]
    # except Exception as e:
    #     print(repo.full_name, "error parsing assignees!!!\n", str(e))
    #     pass

    # Iterate over comments on all commits in the repository.
    try:
        print(repo.full_name, "get comments")
        comments = repo.comments()
        metadata["comments_count"] = len(list(comments))
        metadata["comments"] = None
    except Exception as e:
        print(repo.full_name, "error parsing comments!!!\n", str(e))
        pass

    try:
        print(repo.full_name, "get issues")
        issues = get_client("search").search_issues("repo:" + repo.full_name + " is:issue", number=1)
        for i in issues:
            continue
        metadata["issues_count"] = issues.total_count
        # issues = repo.issues(state="all")
        # metadata["issues_count"] = len(list(issues))
    except Exception as e:
        print(repo.full_name, "error parsing issues!!!\n", str(e))
        pass

    try:
        print(repo.full_name, "get pull_requests")
        pull_requests = get_client("search").search_issues("repo:" + repo.full_name + " is:pr", number=1)
        for i in pull_requests:
            continue
        metadata["pull_requests_count"] = pull_requests.total_count
        # pull_requests = repo.pull_requests(state='all')
        # metadata["pull_requests_count"] = len(list(pull_requests))
    except Exception as e:
        print(repo.full_name, "error parsing pull_requests!!!\n", str(e))
        pass

    try:
        print(repo.full_name, "get commits")
        # commits = get_client("search").search_commits("repo:"+repo.full_name+" is:public", number=1)
        # for i in commits:
        #     continue
        metadata["commits_count"] = get_commits(repo.full_name)
    except Exception as e:
        print(repo.full_name, "error parsing commits!!!\n", str(e))
        # metadata["commits_count"] = len([ c.as_dict() for c in repo.commits() ])
        pass

    try:
        print(repo.full_name, "get travis")
        travis = get_client("search").search_code("travis in:path repo:" + repo.full_name, number=1)
        for i in travis:
            continue
        if travis.total_count > 0:
            metadata["travis"] = True
        else:
            metadata["travis"] = False
    except Exception as e:
        print(repo.full_name, "error getting travis!!!\n", str(e))
        metadata["travis"] = False
        pass

    try:
        print(repo.full_name, "get jenkins")
        jenkins = get_client("search").search_code("jenkins in:path repo:" + repo.full_name, number=1)
        for i in jenkins:
            continue
        if jenkins.total_count > 0:
            metadata["jenkins"] = True
        else:
            metadata["jenkins"] = False
    except Exception as e:
        print(repo.full_name, "error getting jenkins!!!\n", str(e))
        metadata["jenkins"] = False
        pass

    try:
        print(repo.full_name, "get manifest")
        manifest = get_client("search").search_code("manifest in:path repo:" + repo.full_name, number=1)
        for i in manifest:
            continue
        if manifest.total_count > 0:
            metadata["manifest"] = True
        else:
            metadata["manifest"] = False
    except Exception as e:
        print(repo.full_name, "error getting manifest!!!\n", str(e))
        metadata["manifest"] = False
        pass

    try:
        print(repo.full_name, "get Dockerfile")
        manifest = get_client("search").search_code("Dockerfile in:path repo:" + repo.full_name, number=1)
        for i in manifest:
            continue
        if manifest.total_count > 0:
            metadata["Dockerfile"] = True
        else:
            metadata["Dockerfile"] = False
    except Exception as e:
        print(repo.full_name, "error getting Dockerfile!!!\n", str(e))
        metadata["Dockerfile"] = False
        pass

    # CONTRIBUTING.md
    if "ibm" not in API_URL:
        # GET /repos/:owner/:repo/community/profile
        metadata["community"] = get_community_profile(repo.full_name)

        if "community" in metadata and metadata["community"] is not None and "files" in metadata["community"] and \
                metadata["community"]["files"]["contributing"] is not None:
            print(repo.full_name, "get contributing")
            u = metadata["community"]["files"]["contributing"]["html_url"]
            u = u.replace("/blob", "").replace("github.com", "raw.githubusercontent.com")
            try:
                my_repo_doc.put_attachment("CONTRIBUTING.md", "text/markdown; charset=utf-8", requests.request("GET", u).text.encode('utf-8'))
            except:
                print("error downloading CONTRIBUTING.md")
    my_repo_doc = save_doc(repo.full_name, metadata)

    print(15 * "-", "get owner")
    get_user(repo.owner, overwrite, details=user_details)
    if get_users:
        print(15 * "-", "get contributors")
        for c in repo.contributors():
            get_user(c, overwrite, details=user_details)
        if not contributors_only:
            print(15 * "-", "get subscribers")
            for s in repo.subscribers():
                get_user(s, overwrite, details=user_details)
            print(15 * "-", "get stargazers")
            for s in repo.stargazers():
                get_user(s, overwrite, details=user_details)

    return my_repo_doc


def get_repo(repo_org, repo_name):
    try:
        return get_client().repository(repo_org, repo_name)
    except github3.exceptions.NotFoundError as nfe:
        print("repository not found!!!", repo_org, repo_name)
        raise nfe


def get_repo_by_fullname(fullname):
    parts = fullname.split("/")
    return get_repo(parts[0], parts[1])


def get_user_commits(user_login, orgs):
    results = []
    for org in [user_login] + orgs:
        commits = search_commits(user_login, org)
        results += commits
        # results += [ c["repo"] for c in commits ] # cleanup
    return results


def search_commits(committer, org=None, from_date=2020):
    commits = []
    last_updated_at = moment.now().add(week=1).zero.strftime(ISO_SHORT_FORMAT)
    total = 0
    PAGE_SIZE = 100
    res_items = PAGE_SIZE
    while res_items == PAGE_SIZE:
        query = "committer:{} author:{} {}".format(committer, committer, "org:" + org if org is not None else "")
        query = "committer-date:{}..{} sort:committer-date-desc {}".format(from_date, moment.date(last_updated_at).add(
            second=-1).strftime(ISO_FORMAT), query)
        print(query)
        commits_search_result = get_client("search").search_commits(query, number=PAGE_SIZE)
        res_items = 0
        for com in commits_search_result:
            c = com.as_dict()
            res_items += 1
            commit = {
                "repo": c["repository"]["full_name"],
                "date": format_date_utc_iso(c["commit"]["committer"]["date"]),
            }
            commits.append(commit)
            last_updated_at = min(last_updated_at, commit["date"])
            # print(len(commits), res_items, commit, last_updated_at)

        if total == 0:
            total = commits_search_result.total_count
            print("total_commits", total)

    print("commits", len(commits))
    return commits


def get_user_search_issues(user_login, verb, type="issue", gh_client=None):
    if verb not in ["involves", "author", "assignee", "mentions", "commenter"]:
        raise Exception("Invalid verb")
    if type not in ["issue", "pr"]:
        raise Exception("Invalid type")
    gh = gh_client if gh_client is not None else get_client()
    issues = gh.search_issues(verb + ":" + user_login + " is:" + type, number=1)
    for i in issues:
        continue
    return issues.total_count


def get_user_issues_involved(user_login, gh_client=None):
    return get_user_search_issues(user_login, "involves", "issue", gh_client)


def get_user_pull_requests_involved(user_login, gh_client=None):
    return get_user_search_issues(user_login, "involves", "pr", gh_client)


def update_starred_events(repo_name):
    stargazer_event_array = get_starred_events(repo_name)
    save_doc(repo_name, {"starred_events": stargazer_event_array})


def graphql_api(body):
    return requests.request("POST",
                                   HTTP_API_URL + "/graphql",
                                   json={'query': body},
                                   headers={
                                       "Content-Type": "application/json",
                                       "Authorization": get_authorization_header()
                                   }).json()

def get_starred_events(repo_name, cut_date="2000"):
    print(repo_name, " get stargazers' starred time ")
    repo_name_parts = repo_name.split("/")
    try:
        stargazers = {}
        PAGE_SIZE = 100
        hasNextPage = True
        cursor = None
        while hasNextPage:
            print(len(stargazers.keys()), cursor)
            body = """
                {
                    repository(name: "%s", owner: "%s") {
                        stargazers(orderBy: %s, first: %d %s) {
                            edges {
                                starredAt
                                node {
                                    login
                                }
                            }
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                    }
                }""" % (repo_name_parts[1], repo_name_parts[0], "{field: STARRED_AT, direction: DESC}", PAGE_SIZE,
                        ", after: \"{}\"".format(cursor) if cursor else "")
            res = graphql_api(body)
            if "errors" in res:
                print(json.dumps(res["errors"], indent=2))
            try:
                for r in res["data"]["repository"]["stargazers"]["edges"]:
                    if r:
                        starredAt = format_date_utc_iso(r["starredAt"])
                        if starredAt < cut_date:
                            break
                        stargazers[r["node"]["login"]] = starredAt
                page = res["data"]["repository"]["stargazers"]["pageInfo"]
                cursor = page["endCursor"]
                hasNextPage = page["hasNextPage"]
            except Exception as e:
                print(str(e))
                raise e
        print("stargazers", len(stargazers))
        return stargazers

    except Exception as e:
        print(repo_name, "error \n", str(e))
        raise e


def get_commit_history(repo_name, cut_date="2000"):
    print(repo_name, " get commit history ")
    repo_name_parts = repo_name.split("/")
    print(repo_name_parts)
    try:
        commits = []
        PAGE_SIZE = 100
        cursor = None
        hasNextPage = True
        page = 1
        while hasNextPage:
            print("PAGE: ", page)
            body = """
            { 
                repository(name: "%s", owner: "%s") {
                    defaultBranchRef {
                        target {
                            ... on Commit {
                                  history(first: %d %s) {
                                        pageInfo {
                                              hasNextPage
                                              endCursor
                                        }
                                        nodes {
                                              committer {
                                                    date
                                                    user {
                                                          login
                                                    }             
                                              }
                                              message
                                        }
                                  }
                            }
                        }
                    }
                }
            }
            """ % (
            repo_name_parts[1], repo_name_parts[0], PAGE_SIZE, ", after: \"{}\"".format(cursor) if cursor else "")
            res = graphql_api(body)
            if "errors" in res:
                print(json.dumps(res["errors"], indent=2))
            try:
                for r in res["data"]["repository"]["defaultBranchRef"]["target"]["history"]["nodes"]:
                    if r:
                        committedDate = format_date_utc_iso(r["committer"]["date"])
                        if committedDate < cut_date:
                            break
                        committedMsg = r["message"]
                        committerLogin = r["committer"]["user"]["login"] if r["committer"]["user"] is not None else None
                        commits.append({"date": committedDate, "message": committedMsg, "committer": committerLogin})
                cursor = res["data"]["repository"]["defaultBranchRef"]["target"]["history"]["pageInfo"]["endCursor"]
                hasNextPage = res["data"]["repository"]["defaultBranchRef"]["target"]["history"]["pageInfo"]["hasNextPage"]
            except Exception as e:
                print(str(e))
                hasNextPage = False
                pass
            page = page + 1
        print("commits", len(commits))
        return commits

    except Exception as e:
        print(repo_name, "error \n", str(e))
        raise e


def get_issues_history(repo_name, cut_date="2000"):
    print(repo_name, " get issue history ")
    repo_name_parts = repo_name.split("/")
    print(repo_name_parts)
    try:
        issues = []
        PAGE_SIZE = 100
        cursor = None
        hasNextPage = True
        page = 1
        while hasNextPage:
            print("PAGE: ", page)
            body = """
            {
                repository(owner:"%s", name: "%s") {
                    issues(orderBy: %s, first: %d %s ) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        nodes {
                            createdAt
                            author {
                                login
                            }
                            closedAt
                            title
                            closed
                        }
                    }
                }
                }
            """ % (repo_name_parts[0],
                   repo_name_parts[1],
                   "{field: CREATED_AT, direction: DESC}",
                   PAGE_SIZE,
                   ", after: \"{}\"".format(cursor) if cursor else "")
            res = graphql_api(body)
            if "errors" in res:
                print(json.dumps(res["errors"], indent=2))
            try:
                for r in res["data"]["repository"]["issues"]["nodes"]:
                    if r:
                        createdAt = format_date_utc_iso(r["createdAt"])
                        if createdAt < cut_date:
                            break
                        title = r["title"]
                        closed = r["closed"]
                        issuer = r["author"]["login"] if r["author"] else None # TODO confirm if want to add none author in db
                        closedAt = format_date_utc_iso(r["closedAt"]) if r["closedAt"] else None
                        issue = {"title": title, "issuer": issuer,  "closedAt": closedAt, "createdAt": createdAt, "closed": closed}
                        issues.append(issue)
                cursor = res["data"]["repository"]["issues"]["pageInfo"]["endCursor"]
                hasNextPage = res["data"]["repository"]["issues"]["pageInfo"]["hasNextPage"]
            except Exception as e:
                print(str(e))
                hasNextPage = False
                pass
            page = page + 1
        print("Total issues", len(issues))
        return issues

    except Exception as e:
        print(repo_name, "error \n", str(e))
        raise e


def get_fork_history(repo_name, cut_date="2000"):
    print(repo_name, " get forks history ")
    repo_name_parts = repo_name.split("/")
    print(repo_name_parts)
    try:
        forks = {}
        PAGE_SIZE = 100
        cursor = None
        hasNextPage = True
        page = 1
        while hasNextPage:
            print("PAGE: ", page)
            body = """
                 {
                    repository(owner: "%s", name: "%s") {
                    forks(orderBy: %s, first: %d %s ) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        nodes {
                            name
                            createdAt
                            owner {
                                login
                                }
                            }
                        }
                    }
                }
            """ % (repo_name_parts[0],
                   repo_name_parts[1],
                   "{field: CREATED_AT, direction: DESC}",
                   PAGE_SIZE,
                   ", after: \"{}\"".format(cursor) if cursor else "")
            res = graphql_api(body)
            if "errors" in res:
                print(json.dumps(res["errors"], indent=2))
            try:
                for r in res["data"]["repository"]["forks"]["nodes"]:
                    if r:
                        createdAt = format_date_utc_iso(r["createdAt"])
                        if createdAt < cut_date:
                            break
                        forks[r["owner"]["login"]] = createdAt
                cursor = res["data"]["repository"]["forks"]["pageInfo"]["endCursor"]
                hasNextPage = res["data"]["repository"]["forks"]["pageInfo"]["hasNextPage"]
            except Exception as e:
                print(str(e))
                hasNextPage = False
                pass
            page = page + 1
        print("Total forks", len(forks))
        return forks

    except Exception as e:
        print(repo_name, "error \n", str(e))
        raise e


def get_watcher_events(repo_name, cut_date="2000"):
    print(repo_name, " get watchers' time ")
    repo_name_parts = repo_name.split("/")
    try:
        watchers = {}
        PAGE_SIZE = 100
        cursor = None
        hasNextPage = True
        while hasNextPage:
            print(len(watchers.keys()), cursor)
            body = """
                {
                    repository(owner: "%s", name: "%s") {
                        watchers( first: %d %s) {
                            nodes {
                                login
                                createdAt
                            }
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                    }
                }""" % (repo_name_parts[0],
                        repo_name_parts[1],
                        PAGE_SIZE,
                        ", after: \"{}\"".format(cursor) if cursor else "")
            res = graphql_api(body)

            if "errors" in res:
                print(json.dumps(res["errors"], indent=2))
            try:
                for r in res["data"]["repository"]["watchers"]["nodes"]:
                    if r:
                        createdAt = format_date_utc_iso(r["createdAt"])
                        if createdAt < cut_date:
                            break
                        watchers[r["login"]] = createdAt
                page = res["data"]["repository"]["watchers"]["pageInfo"]
                cursor = page["endCursor"]
                hasNextPage = page["hasNextPage"]
            except Exception as e:
                print(str(e))
                raise e
        print("watchers", len(watchers))
        return watchers

    except Exception as e:
        print(repo_name, "error \n", str(e))
        raise e


def get_README_history(repo_name, releases,  cut_date="2000"):
    print(repo_name, " get README history ")
    repo_name_parts = repo_name.split("/")
    response = {}
    tags = ["HEAD"] if releases is None else [r['tag'] for r in releases]
    defaultBranchRef = "main"
    # print(tags)
    try:
        content = {}
        for tag in tags:
            body = """
                {
                    repository(owner: "%s", name: "%s") {
                        content: object(expression: "%s:readme.md") {
                            ... on Blob {
                                text
                                byteSize
                            }
                        }
                         defaultBranchRef {
                          name
                        }
                    }
                }""" % (repo_name_parts[0], repo_name_parts[1], tag)
            res = graphql_api(body)
            r = res["data"]["repository"]
            content[tag] = {'text': r['content']['text'], 'byteSize': r['content']['byteSize']}
            defaultBranchRef = r['defaultBranchRef']['name']

        response['content'] = content
        response['defaultBranchRef'] = defaultBranchRef

        commits = []
        PAGE_SIZE = 100
        cursor = None
        hasNextPage = True
        while hasNextPage:
            body = """
            {
                repository(owner: "%s", name: "%s") {
                    info: ref(qualifiedName: "%s") {
                        target {
                            ... on Commit {
                                history(path: "README.md", first: %d %s ) {
                                    nodes {
                                        author {
                                            user {
                                                login
                                            }
                                        }
                                        id
                                        additions
                                        deletions
                                        resourcePath
                                        url
                                        committedDate
                                    }
                                    pageInfo {
                                              hasNextPage
                                              endCursor
                                    }
                                }
                            }
                        }
                    }
                }
            }""" % (repo_name_parts[0],
                    repo_name_parts[1],
                    defaultBranchRef,
                    PAGE_SIZE,
                    ", after: \"{}\"".format(cursor) if cursor else "",
                    )
            res = graphql_api(body)
            if "errors" in res:
                print(json.dumps(res["errors"], indent=2))
            r = res["data"]["repository"]['info']
            if r:
                history = r['target']['history']
                nodes = history['nodes']
                for n in nodes:
                    committedDate = format_date_utc_iso(n["committedDate"])
                    if committedDate < cut_date:
                        break
                    url = n['url']
                    commits.append({
                        'committeDate': committedDate,
                        'login': n['author']['user']['login'] if n['author']['user'] else None,
                        'additions': n['additions'],
                        'deletions': n['deletions'],
                        'url': n['url']
                    })
                    # todo recursively make a post request to get the history of each commit url

                page = history["pageInfo"]
                cursor = page["endCursor"]
                hasNextPage = page["hasNextPage"]
        response['history'] = commits
        print("README history", len(response))
        return response

    except Exception as e:
        print(repo_name, "error \n", str(e))
        raise e
