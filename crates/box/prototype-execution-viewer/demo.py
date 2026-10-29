#!/usr/bin/env python3
"""Run a throwaway Box workload and capture its component trace."""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile


def main():
    here = Path(__file__).resolve().parent
    repository = here.parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=repository / "target/debug/strands-box")
    parser.add_argument("--output", type=Path, help="Write the standalone playback HTML here.")
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error("Build with: cargo build -p strands-box -p strands-box-containment --all-features")

    root = Path(tempfile.mkdtemp(prefix="box-viz-", dir="/var/tmp")).resolve()
    project = root / "project"
    operator = root / "operator"
    agent_home = operator / "agent-home"
    tools = operator / "tools"
    telemetry_path = operator / "box" / "private" / "telemetry" / "records.jsonl"
    for directory in (project, agent_home, tools):
        directory.mkdir(parents=True)
    (project / "input.txt").write_text("alpha\nbeta\ngamma\n")
    sleep = shutil.which("sleep")
    if not sleep:
        parser.error("The demo needs a host sleep executable.")
    curl = shutil.which("curl")
    if not curl:
        parser.error("The demo needs a host curl executable.")
    curl = Path(curl).resolve()
    for name in ("build-check", "scan-files"):
        shutil.copy2(sleep, tools / name)

    policy = """\
permit(principal, action == Box::Action::"shell:exec", resource);
permit(principal, action == Box::Action::"shell:spawn", resource);
permit(principal, action == Box::Action::"fs:read", resource);
permit(principal, action == Box::Action::"net:connect", resource)
when { context.input.host == "example.com" && context.input.port == 443 };
permit(principal, action == Box::Action::"http:request", resource)
when {
    context.input.host == "example.com"
    && context.input.port == 443
    && context.input.method == "GET"
    && context.input.path == "/"
};
"""
    (root / "policy.dw").write_text(policy)
    command = [
        "/bin/bash", "--noprofile", "--norc", "-c",
        """\
set -e
printf 'Starting the execution-viewer demo\\n'
printf 'Native agent HTTPS request\\n'
__CURL__ --disable --fail --silent --show-error --cacert "$SSL_CERT_FILE" https://example.com/
printf '\\nBrokered Shell HTTPS request\\n'
zsh -c 'curl https://example.com/'
zsh -c 'cat input.txt'
zsh -c '__TOOLS__/build-check 1.0' &
build_pid=$!
zsh -c '__TOOLS__/scan-files 0.5' &
scan_pid=$!
python3 -c "print('Monty read: ' + open('input.txt').read())"
zsh -c 'printf refused > denied.txt' || printf 'Write refused as expected\\n'
wait "$scan_pid"
wait "$build_pid"
printf 'Execution-viewer demo complete\\n'
""".replace("__TOOLS__", str(tools)).replace("__CURL__", shlex.quote(str(curl))),
    ]
    quoted = json.dumps
    config = f"""\
name = "execution-viewer-demo"
box_dir = {quoted(str(operator / "box"))}
policy = "policy.dw"

[agent]
command = {quoted(command)}
workspace = {quoted(str(project))}
env = {{ HOME = {quoted(str(agent_home))}, PATH = {quoted(str(tools) + ":/usr/bin:/bin")}, TERM = "dumb" }}

[agent.filesystem]
read = [{quoted(str(project))}]
exec = [{quoted(str(curl))}]
"""
    for name in ("build-check", "scan-files"):
        config += f"""
[tool.{name}]
command = [{quoted(str(tools / name))}]
workspace = {quoted(str(project))}
env = {{ PATH = "/usr/bin:/bin" }}
"""
    config += f"""
[telemetry.local]
kind = "file"
destination = {quoted(str(telemetry_path))}
include = ["deny", "permit", "trace", "logs", "metrics", "execution"]
"""
    config_path = root / "box.toml"
    config_path.write_text(config)
    environment = dict(os.environ)
    environment["HOME"] = str(operator)
    print(f"Demo files: {root}", flush=True)
    print(f"Telemetry: {telemetry_path}", flush=True)
    result = subprocess.run(
        [str(binary), "run", "--config", str(config_path)],
        cwd=project, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, timeout=120,
    )
    (root / "stdout.log").write_text(result.stdout)
    (root / "stderr.log").write_text(result.stderr)
    print(result.stdout, end="")
    print(result.stderr, end="")
    if telemetry_path.is_file() and telemetry_path.stat().st_size:
        print(f"Captured {len(telemetry_path.read_text().splitlines())} telemetry lines.")
        if (here / "viewer.html").is_file():
            output = args.output.resolve() if args.output else root / "playback.html"
            subprocess.run(
                ["python3", str(here / "render.py"), str(telemetry_path), str(output),
                 "--name", "Box execution · recorded demo"],
                check=True,
            )
        else:
            print("The viewer template is missing. The telemetry file is ready for render.py.")
    else:
        print('No telemetry was written. Check the target and its "execution" include.')
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
