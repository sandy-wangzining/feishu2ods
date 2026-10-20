# -*- coding: utf-8 -*-
"""从 api2ods/shared 同步公共源码；--check 离线校验本仓库或指定仓库。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path

PROJECTS = ("sftp2ods", "api2ods", "feishu2ods")
ROOT = Path(__file__).resolve().parents[1]
MANIFEST = "shared-runtime.json"


def file_map(package: str) -> dict[str, str]:
    """公共源文件与仓库内副本的位置；运行时仅依赖包内的 _common.py。"""
    return {
        "runtime.py": f"{package}/_common.py",
        "sync_common.py": "tools/sync_common.py",
        "test_runtime.py": "tests/test_shared_runtime.py",
        "README.md": "docs/shared-runtime.md",
    }


def digest(path: Path) -> str:
    """忽略 checkout 的 CRLF 差异，校验实际源码内容。"""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def write_atomic(path: Path, content: bytes) -> None:
    """逐文件替换，防止中断或并发 import 读到被截断的源码；保留原文件权限。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="api2ods/shared 源目录；同步时必需，本仓库有 shared 时默认使用它")
    parser.add_argument("--check", action="store_true", help="只校验，不修改任何文件")
    parser.add_argument("repos", type=Path, nargs="*", help="仓库目录，可一次指定三个；默认本仓库")
    args = parser.parse_args(argv)
    source = args.source or (ROOT / "shared" if (ROOT / "shared/runtime.py").is_file() else None)
    if source is None and not args.check:
        parser.error("同步需要 --source /path/to/api2ods/shared；单仓库离线校验请用 --check")
    repos = []
    for repo in args.repos or [ROOT]:
        repo = repo.resolve()
        packages = [name for name in PROJECTS if (repo / name / "__init__.py").is_file()]
        if len(packages) != 1:
            parser.error(f"无法识别 ODS 仓库：{repo}")
        files = file_map(packages[0])
        for target in [*files.values(), MANIFEST]:
            if repo not in (repo / target).resolve().parents:
                parser.error(f"公共文件通过软链指向仓库外，拒绝覆盖：{repo / target}")
        repos.append((repo, files))
    if source is not None:
        for name in file_map("api2ods"):
            path = source / name
            if not path.is_file():
                parser.error(f"公共源文件缺失：{path}")
            if path.suffix == ".py":
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
    failed = False
    for repo, files in repos:
        if args.check:
            try:
                manifest = json.loads((repo / MANIFEST).read_text(encoding="utf-8"))
                expected = manifest["files"]
                if set(expected) != set(files.values()):
                    raise ValueError("公共文件清单不一致")
                for name, target in files.items():
                    actual = digest(repo / target)
                    if actual != expected[target] or (source is not None and actual != digest(source / name)):
                        raise ValueError(f"公共源码发生漂移：{target}")
            except (OSError, ValueError, KeyError, TypeError) as exc:
                print(f"FAIL {repo}: {exc}")
                failed = True
                continue
        else:
            for name, target in files.items():
                path = repo / target
                write_atomic(path, (source / name).read_bytes())
            manifest = {
                "source": "https://github.com/sandy-wangzining/api2ods/tree/main/shared",
                "files": {target: digest(repo / target) for target in files.values()},
            }
            write_atomic(repo / MANIFEST, (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))
        print(f"OK {repo}")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
