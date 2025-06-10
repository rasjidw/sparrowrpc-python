#!/usr/bin/env python3

import json
import pathlib


DEPS = [
    ["__future__", "latest"],
    ["collections", "latest"],
    ["collections-defaultdict", "latest"],
    ["datetime", "latest"],
    ["inspect", "latest"],
    ["logging", "latest"],
    ["shutil", "latest"],
    ["tempfile", "latest"],
    ["traceback", "latest"],
    ["github:rasjidw/binarychain-python", "main"],
    ["github:josverl/micropython-stubs/mip/typing.py", "main"],
    ["github:rasjidw/udataclasses", "main"]
]


GITHUB_REPO = 'github:rasjidw/sparrowrpc-python/'


def main():
    repo_root = pathlib.Path(__file__).parent
    micropython_dir = repo_root / 'libmpy'

    urls = list()
    for dirpath, dirs, filenames in micropython_dir.walk():
        for filename in filenames:
            if filename.endswith('.py'):
                src_relative_dir = dirpath.relative_to(repo_root)
                src_path = src_relative_dir / filename
                dst_relative = dirpath.relative_to(micropython_dir)
                dst_path = dst_relative / filename
                urls.append([str(dst_path), GITHUB_REPO + str(src_path)])

    src_dir = repo_root / 'src'
    sparrowrpc_dir = src_dir / 'sparrowrpc'
    for dirpath, dirs, filenames in sparrowrpc_dir.walk():
        for dirname in dirs.copy():
            if dirname == 'threaded' or dirname[0] in ('.', '_'):
                dirs.remove(dirname)
        print(f'including files in {dirpath}')
        for filename in filenames:
            if filename.endswith('.py'):
                src_relative_dir = dirpath.relative_to(repo_root)
                src_path = src_relative_dir / filename
                dst_relative = dirpath.relative_to(src_dir)
                dst_path = dst_relative / filename
                urls.append([str(dst_path), GITHUB_REPO + str(src_path)])

    package_data = dict(urls=urls, deps=DEPS)
    with open('package.json', 'w') as f:
        json.dump(package_data, f, indent=2)


if __name__ == '__main__':
    main()
