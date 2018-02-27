
import os.path
import subprocess
import shutil
import datetime
import github3
import getpass

RAIN_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_DIR = os.path.join(RAIN_ROOT, "dist", "build")

def call(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    out, err = p.communicate()
    assert p.wait() == 0
    return out.decode()

def main():
    os.chdir(RAIN_ROOT)
    now = datetime.datetime.now()
    git_commit = call(("git", "rev-parse", "HEAD"))
    build_name = "rain-{}-{}-{}-linux-x64".format(now.year, now.month, now.day)

    print("Building", build_name)

    subprocess.check_call(("cargo", "build", "--release"))
    os.chdir(os.path.join(RAIN_ROOT, "python"))
    shutil.rmtree("dist")
    subprocess.check_call(("python3", "setup.py", "bdist_wheel"))
    dist_files = list(os.listdir("dist"))
    assert len(dist_files) == 1
    wheel = dist_files[0]
    wheel_path = os.path.join(RAIN_ROOT, "python", "dist", wheel)

    if os.path.isdir(BUILD_DIR):
        print("Removing old", BUILD_DIR)
        shutil.rmtree(BUILD_DIR)

    print("Creating new", BUILD_DIR)
    os.makedirs(BUILD_DIR)

    release_dir = os.path.join(BUILD_DIR, build_name)
    os.makedirs(release_dir)

    print("Copying binary")
    subprocess.check_call(("cp",
                           os.path.join(RAIN_ROOT, "target", "release", "rain"),
                           os.path.join(release_dir, build_name)))

    print("Creating archieve")

    tarball = build_name + ".tar.xz"
    tarball_path = os.path.join(BUILD_DIR, tarball)
    subprocess.check_call(("tar", "cvJf",
                           tarball,
                           build_name), cwd=BUILD_DIR)
    print("Publishing")

    tag_name = "nightly-{}-{}-{}".format(now.year, now.month, now.day)
    name = tag_name
    description = "Nightly build"

    login = input("Github login:")
    password = getpass.getpass("Github password:")
    g = github3.login(login, password)
    repo = g.repository("substantic", "rain")

    release = repo.create_release(tag_name, git_commit, name, description, prerelease=True)
    with open(tarball_path, "rb") as f:
        release.upload_asset("application/x-xz", tarball, f.read())
    with open(wheel_path, "rb") as f:
        release.upload_asset("application/zip", wheel, f.read())

if __name__ == "__main__":
    main()
