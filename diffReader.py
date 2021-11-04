from io import StringIO

from unidiff import PatchSet
from unidiff.patch import Line

import utils.github_utils as gh
import requests
import sys


# https://api.github.com/repos/IBM/Plex/commits/e83ff17d7d8a0db97b3c3bb87c530cb900c8d315


def get_lines(patch_set):
    # print('---> ', patch_set)
    change_list = []  # list of changes
    # [(file_name, [row_number_of_deleted_line],
    # [row_number_of_added_lines]), ... ]

    for patched_file in patch_set:
        file_path = patched_file.path  # file name
        print('file name :' + file_path)
        del_line_no = [line.target_line_no
                       for hunk in patched_file for line in hunk
                       if line.is_added and
                       line.value.strip() != '']  # the row number of deleted lines
        print('deleted lines : ' + str(del_line_no))
        ad_line_no = [line.source_line_no for hunk in patched_file
                      for line in hunk if line.is_removed and
                      line.value.strip() != '']  # the row number of added liens
        print('added lines : ' + str(ad_line_no))
        change_list.append((file_path, del_line_no, ad_line_no))


def get_diff_details(patch_set):
    diff_contents = []
    for p in patch_set:
        # print ('patch::', p)
        if p.added > 0:
            contents = []

            for h in p:
                # print('hunk:: ', h)
                added = []
                for i, line in enumerate(h):
                    if line.is_added:
                        added_line = Line(line.target_line_no, line.value, i + 1)
                        added.append(added_line)
                contents += added
            diff_contents.append(
                # DiffContent(p.path, contents)
                contents
            )
    print(diff_contents)
    return diff_contents


def get_diff_patch(full_name, commitId):
    print(full_name, "get diff path")
    try:
        res = requests.request("GET",
                               gh.API_URL + "/repos/" + full_name + "/commits/" + commitId,
                               headers={"Accept": "application/vnd.github.v3.diff"},
                               auth=('', gh.get_token()))
        patch_set = PatchSet(StringIO(res.content.decode("UTF-8")))
        # print(patch_set)
        get_lines(patch_set)
        get_diff_details(patch_set)

    except Exception as e:
        print(str(e))
        return {}


if __name__ == "__main__":
    get_diff_patch('IBM/fhe-toolkit-linux', 'bd1ae10b0b01635ab90a9b516b884848cf37ae68')
