#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import os, traceback
from utils.cloudant_utils import cloudant_db
import utils.github_utils as gh

OUTPUT_DIR = "data/"

def main():
    global OUTPUT_DIR
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    process()


def process():
    try:
        selector = {'type': {'$eq': 'Repo'}}
        docs = cloudant_db.get_query_result(selector)
        for doc in docs:
            for key in ['subscribers_id', 'contributors_id', 'stargazers_id']:
                # print(key, " : ", doc[key])
                selector = {'_id': {'$in': doc[key]}}
                fields = ['_id', 'company', 'company', 'name', 'followers', 'following', 'read', 'readme', 'full_name',
                          'description']  # Todo Fields to be updated
                user_docs = cloudant_db.get_query_result(selector, fields)
                # print(user_docs[:])
                doc[key] = user_docs[:]

            print(doc)

            # 2. create a read_me file
            readme_filepath = save_Read_me_file(doc)

            # add/update the read_me file and big fat metadata
            # save_FAT_metadata(doc, readme_filepath)

    except Exception as e:
        traceback.print_exc()
        pass


def save_FAT_metadata(user_docs, readme_filepath):
    # TODO work on existing repos
    global existing_repos
    global existing_repos_pushed_at

    # if user_docs.full_name in existing_repos:
    #     d1.discovery_update_file(existing_repos[user_docs.full_name], readme_filepath, "text/html", user_docs)
    # else:
    #     d1.discovery_add_file(readme_filepath, "text/html", user_docs)


def save_Read_me_file(doc):
    global OUTPUT_DIR
    readme = doc.get('readme', None)
    if readme is None or len(str(readme)) == 0:
        print("No readme available!\n\n")
        pass

    file_path = os.path.join(OUTPUT_DIR, gh.file_name(doc['name'], "html"))

    if not os.path.exists(file_path):
        with open(file_path, 'w') as outfile:
            outfile.write("<html>\n")
            outfile.write("<head>\n")
            outfile.write("<meta content=\"text/html; charset=UTF-8\" http-equiv=\"Content-Type\"/>\n")
            outfile.write("<title>")
            outfile.write(doc['description'].strip() if doc['description'] is not None else doc['name'])
            outfile.write("</title>\n")
            outfile.write("</head>\n")
            outfile.write("<body>\n")
            outfile.write(readme)
            outfile.write("\n</body>\n")
            outfile.write("</html>\n")

    return file_path


if __name__ == "__main__":
    main()
