"""
Metadata
========

Contains functions for extracting additional metadata about the current project

"""

import typing
import git
import json


def git_info(repository: str) -> dict[str, typing.Any]:
    """Retrieves metadata for the target git repository

    This is a passive function which returns an empty dictionary if any
    metadata is missing. Exceptions are raised only if the repository
    does not have the expected form assumed by this retrieval method.

    Parameters
    ----------
    repository : str
        root directory of the git repository

    Returns
    -------
    dict[str, typing.Any]
        metadata for the target repository
    """
    try:
        git_repo = git.Repo(repository, search_parent_directories=True)
    except git.InvalidGitRepositoryError:
        return {}
    current_commit: git.Commit = git_repo.head.commit
    author_list: set[str] = set(
        email
        for commit in git_repo.iter_commits("--all")
        if "noreply" not in (email := (commit.author.email or ""))
        and "[bot]" not in (commit.author.name or "")
    )

    ref: str = current_commit.hexsha

    # In the case where the repository is dirty blame should point to the
    # current developer, not the person responsible for the latest commit
    if dirty := git_repo.is_dirty():
        blame = git_repo.config_reader().get_value("user", "email")
    else:
        blame = current_commit.committer.email

    for tag in git_repo.tags:
        if tag.commit == current_commit:
            ref = tag.name
            break

    return {
        "git.authors": json.dumps(list(author_list)),
        "git.ref": ref,
        "git.msg": current_commit.message.strip(),
        "git.time_stamp": current_commit.committed_datetime.strftime(
            "%Y-%m-%d %H:%M:%S %z UTC"
        ),
        "git.blame": blame,
        "git.url": git_repo.remote().url,
        "git.dirty": dirty,
    }


if __name__ in "__main__":
    import os.path
    import json

    print(json.dumps(git_info(os.path.dirname(__file__)), indent=2))
