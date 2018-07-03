import os
import subprocess
import tempfile
from shutil import copytree, rmtree

import git

RAIN_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPO_URL = "https://github.com/substantic/rain"


def call(args, **kwargs):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, **kwargs)
    out, err = p.communicate()
    assert p.wait() == 0
    return out.decode()


def main():
    call(["make", "html"], cwd=os.path.join(RAIN_ROOT, "docs"))

    with tempfile.TemporaryDirectory() as tmp:
        repo = git.Repo.clone_from(REPO_URL, tmp, depth=1, branch="gh-pages")

        target_path = os.path.join(tmp, "docs")
        rmtree(target_path, ignore_errors=True)
        copytree(os.path.join(RAIN_ROOT, "docs/guide-build/html"), target_path)

        if repo.is_dirty(untracked_files=True):
            repo.git.add(target_path)
            repo.git.commit(message="Update")
            repo.git.push()

            print("Documentation uploaded")
        else:
            print("No changes to deploy")


if __name__ == "__main__":
    main()
