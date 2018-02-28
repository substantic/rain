
import os.path
import subprocess
import shutil
import datetime
import github3
import getpass
import sys

RAIN_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_DIR = os.path.join(RAIN_ROOT, "dist", "build")


class ReleaseBase:

    prerelease = False

    def __init__(self):
        self.git_commit = call(("git", "rev-parse", "HEAD"))

    @property
    def dist_name(self):
        return "rain-v{}-linux-x64".format(self.version)


class BasicRelease(ReleaseBase):

    def __init__(self, rain_version):
        super().__init__()
        self.rain_version = rain_version

    @property
    def version(self):
        return self.rain_version

    @property
    def tag_name(self):
        return "v{}".format(self.version)

    @property
    def release_name(self):
        return "v{}".format(self.version)

    @property
    def description(self):
        return "Rain release v{}".format(self.version)


class NighlyRelease(ReleaseBase):

    prerelease = True

    def __init__(self, rain_version):
        super().__init__()
        self.rain_version = rain_version
        self.now = datetime.datetime.now()

    @property
    def version(self):
        return "{}.dev{}{:02}{:02}".format(
            self.rain_version, self.now.year, self.now.month, self.now.day)

    @property
    def tag_name(self):
        return "nightly-v{}".format(self.version)

    @property
    def release_name(self):
        return "nightly-{}".format(self.version)

    @property
    def description(self):
        return "Rain nightly release v{}".format(self.version)


def load_cargo_version():
    with open(os.path.abspath(os.path.join(RAIN_ROOT, "Cargo.toml"))) as f:
        import re
        exp = re.compile('version = "([^"]*)"')
        for line in f:
            m = exp.search(line)
            if m:
                return m.groups()[0]
    raise Exception("Cannot determine version")


def call(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    out, err = p.communicate()
    assert p.wait() == 0
    return out.decode()


def main():
    rain_version = load_cargo_version()
    print("Rain version:", rain_version)

    if len(sys.argv) != 2 or sys.argv[1] not in ["release", "nightly"]:
        sys.stderr.write("Usage: make_release.py [release|nightly]\n")
        return

    if sys.argv[1] == "release":
        info = BasicRelease(rain_version)
    else:
        print("Building NIGHTLY version")
        info = NighlyRelease(rain_version)

    print("Relase version:", info.version)
    print("Git commit:", info.git_commit)

    dist_env = os.environ.copy()
    dist_env["RAIN_VERSION"] = info.version

    os.chdir(RAIN_ROOT)
    subprocess.check_call(("cargo", "build", "--release"))

    os.chdir(os.path.join(RAIN_ROOT, "python"))
    shutil.rmtree("dist")  # Remove dist in python dir (create by setup.py)
    subprocess.check_call(("python3", "setup.py", "bdist_wheel"), env=dist_env)
    dist_files = list(os.listdir("dist"))
    assert len(dist_files) == 1
    wheel = dist_files[0]
    wheel_path = os.path.join(RAIN_ROOT, "python", "dist", wheel)

    print("Wheel:", wheel)

    if os.path.isdir(BUILD_DIR):
        print("Removing old build dir", BUILD_DIR)
        shutil.rmtree(BUILD_DIR)

    print("Creating new build dir", BUILD_DIR)
    os.makedirs(BUILD_DIR)

    release_dir = os.path.join(BUILD_DIR, info.dist_name)
    os.makedirs(release_dir)

    print("Copying binary")
    subprocess.check_call(("cp",
                           os.path.join(RAIN_ROOT, "target", "release", "rain"),
                           os.path.join(release_dir, "rain")))

    print("Creating archieve")

    tarball = info.dist_name + ".tar.xz"
    tarball_path = os.path.join(BUILD_DIR, tarball)
    subprocess.check_call(("tar", "cvJf",
                           tarball,
                           info.dist_name), cwd=BUILD_DIR)
    print("Publishing")

    login = input("Github login:")
    password = getpass.getpass("Github password:")
    g = github3.login(login, password)
    repo = g.repository("substantic", "rain")

    release = repo.create_release(
        info.tag_name, info.git_commit, info.release_name, info.description,
        prerelease=info.prerelease)
    with open(tarball_path, "rb") as f:
        release.upload_asset("application/x-xz", tarball, f.read())
    with open(wheel_path, "rb") as f:
        release.upload_asset("application/zip", wheel, f.read())


if __name__ == "__main__":
    main()
