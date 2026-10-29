#!/usr/bin/env python3
"""Generate explicitly synthetic traces for the throwaway viewer benchmark."""

import argparse
import itertools
import json
from pathlib import Path
import time


EPOCH = 1_700_000_000_000


def otlp_batch(events):
    return {"resourceLogs": [{
        "resource": {"attributes": [
            {"key": "strands.box.run.id", "value": {"stringValue": "synthetic-run"}},
            {"key": "strands.box.name", "value": {"stringValue": "synthetic-benchmark"}},
            {"key": "strands.box.source", "value": {"stringValue": "box"}},
        ]},
        "scopeLogs": [{
            "scope": {"name": "strands-box.execution"},
            "logRecords": [{
                "timeUnixNano": str(int(event["ts"] * 1_000_000)),
                "eventName": "strands.box.execution.event",
                "attributes": [{
                    "key": "strands.box.execution.version", "value": {"intValue": "3"},
                }],
                "body": {"stringValue": json.dumps(event, separators=(",", ":"))},
            } for event in events],
        }],
    }]}


def command_events(index):
    prefix = f"command-{index}"
    broker, shell, leaf = (f"{prefix}:{part}" for part in ("broker", "shell", "leaf"))
    filesystem, gateway = f"{prefix}:filesystem", f"{prefix}:gateway"
    exchange = f"{prefix}:exchange"
    trigger = f"synthetic-command {index}"
    base = EPOCH + index * 2
    rows = [
        ("start", broker, "broker", "agent", "broker", None, "box.sock"),
        ("start", shell, "shell", "broker", "shell", broker, "Shell request"),
        ("decision", f"{prefix}:exec", "policy", "shell", "policy", shell, "shell:exec"),
        ("message", f"{prefix}:exec-result", "policy", "policy", "shell", shell, "Permit returned"),
        ("decision", f"{prefix}:read", "policy", "shell", "policy", shell, "fs:read"),
        ("start", filesystem, "filesystem", "shell", "filesystem", shell, "Open for read"),
        ("end", filesystem, "filesystem", "filesystem", "shell", shell, "Open for read"),
        ("decision", f"{prefix}:spawn", "policy", "shell", "policy", shell, "shell:spawn"),
        ("start", leaf, "leaf", "shell", leaf, shell, f"synthetic-tool-{index}"),
        ("start", gateway, "gateway", "shell", "gateway", shell, "HTTP request"),
        ("decision", f"{prefix}:http", "policy", "gateway", "policy", None, "http:request"),
        ("message", f"{prefix}:http-result", "policy", "policy", "gateway", None, "Permit returned"),
        ("message", f"{prefix}:request", "gateway", "gateway", "internet", None, "HTTP request write attempted"),
        ("message", f"{prefix}:response", "gateway", "internet", "gateway", None, "HTTP response received"),
        ("end", gateway, "gateway", "gateway", "shell", shell, "HTTP request"),
        ("message", leaf, "leaf", leaf, "shell", shell, "Exit status"),
        ("end", leaf, "leaf", leaf, "shell", shell, f"synthetic-tool-{index}"),
        ("message", f"{prefix}:completed", "shell", "shell", "broker", shell, "Command completed"),
        ("end", shell, "shell", "shell", "broker", broker, "Shell request"),
        ("end", broker, "broker", "broker", "agent", None, "box.sock"),
    ]
    for offset, (kind, identifier, component, source, target, parent, label) in enumerate(rows):
        event = {
            "ts": base + offset * 0.5, "kind": kind, "id": identifier,
            "component": component, "from": source, "to": target,
            "parent": parent, "label": label, "exchange": exchange,
        }
        if offset == 0:
            event["trigger"] = trigger
        if component == "filesystem":
            event["detail"] = f"/synthetic/project-{index}/input.txt"
        if kind == "decision":
            event.update(action=label, verdict="permit")
        if kind == "end":
            event["outcome"] = "completed"
        yield event


def single_events(count):
    broker, shell = "command-0:broker", "command-0:shell"
    yield dict(ts=EPOCH, kind="start", id=broker, component="broker", **{
        "from": "agent", "to": "broker", "trigger": "synthetic-command 0", "label": "box.sock",
    })
    yield dict(ts=EPOCH + 1, kind="start", id=shell, component="shell", parent=broker, **{
        "from": "broker", "to": "shell", "label": "Shell request",
    })
    for index in range((count - 4) // 4):
        is_leaf = index % 64 == 0
        component = "leaf" if is_leaf else "filesystem"
        identifier = f"command-0:{component}-{index}"
        target = identifier if is_leaf else "filesystem"
        action = "shell:spawn" if is_leaf else "fs:read"
        label = f"synthetic-tool-{index}" if is_leaf else "Open for read"
        yield dict(ts=EPOCH + 2 + index * 4, kind="decision", id=f"command-0:decision-{index}",
                   component="policy", parent=shell, action=action, label=action, verdict="permit",
                   **{"from": "shell", "to": "policy"})
        yield dict(ts=EPOCH + 3 + index * 4, kind="message", id=f"command-0:result-{index}",
                   component="policy", parent=shell, label="Permit returned", verdict="permit",
                   **{"from": "policy", "to": "shell"})
        yield dict(ts=EPOCH + 4 + index * 4, kind="start", id=identifier, component=component,
                   parent=shell, label=label, detail=f"/synthetic/input-{index}.txt",
                   **{"from": "shell", "to": target})
        yield dict(ts=EPOCH + 5 + index * 4, kind="end", id=identifier, component=component,
                   parent=shell, label=label, detail=f"/synthetic/input-{index}.txt",
                   outcome="completed", **{"from": target, "to": "shell"})
    yield dict(ts=EPOCH + count - 2, kind="end", id=shell, component="shell", parent=broker,
               label="Shell request", **{"from": "shell", "to": "broker"})
    yield dict(ts=EPOCH + count - 1, kind="end", id=broker, component="broker",
               label="box.sock", **{"from": "broker", "to": "agent"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--scenario", choices=("many", "single"), default="many")
    parser.add_argument("--events", type=int, default=500_000)
    parser.add_argument("--wire", choices=("raw", "otlp"), default="raw")
    args = parser.parse_args()
    divisor = 20 if args.scenario == "many" else 4
    if args.events < 20 or args.events % divisor:
        parser.error(f"--events must be at least 20 and divisible by {divisor}.")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    count = args.events // 20 if args.scenario == "many" else 1
    metadata = {
        "kind": "trace_metadata", "source": "synthetic",
        "name": f"Synthetic benchmark: {args.scenario}, {args.events:,} events",
        "scenario": args.scenario, "event_count": args.events, "command_count": count,
        "events_per_command": 20 if args.scenario == "many" else args.events,
        "wire": args.wire,
    }
    started = time.monotonic()
    events = (
        (event for command in range(count) for event in command_events(command))
        if args.scenario == "many" else single_events(args.events)
    )
    with args.output.open("w") as output:
        if args.wire == "raw":
            output.write(json.dumps(metadata, separators=(",", ":")) + "\n")
            for event in events:
                output.write(json.dumps(event, separators=(",", ":")) + "\n")
        else:
            start = {"kind": "capture_start", "capture_version": 3, "ts": EPOCH - 1, "sequence": 0}
            end = {
                "kind": "capture_summary", "capture_version": 3,
                "ts": EPOCH + args.events + 1, "expected_events": args.events,
                "last_sequence": args.events, "outcome": "verify_sequence",
            }
            sequenced = ({**event, "sequence": sequence} for sequence, event in enumerate(events, 1))
            all_records = iter(itertools.chain((start,), sequenced, (end,)))
            while batch := list(itertools.islice(all_records, 64)):
                output.write(json.dumps(otlp_batch(batch), separators=(",", ":")) + "\n")
    metadata["bytes"] = args.output.stat().st_size
    metadata["generate_seconds"] = round(time.monotonic() - started, 3)
    args.output.with_suffix(".meta.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata))


if __name__ == "__main__":
    main()
