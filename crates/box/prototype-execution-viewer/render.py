#!/usr/bin/env python3
"""Embed legacy execution JSONL or Box execution telemetry in the prototype viewer."""

import argparse
import itertools
import json
from pathlib import Path
import re
import tempfile


EXECUTION_SCOPE = "strands-box.execution"
METADATA_KINDS = {"trace_metadata", "capture_start", "capture_loss", "capture_summary"}
OTLP_KEYS = {"resourceLogs", "resourceSpans", "resourceMetrics"}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    ).replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


def any_value(value):
    if not isinstance(value, dict):
        return None
    for key in ("stringValue", "intValue", "doubleValue", "boolValue"):
        if key in value:
            return value[key]
    if "arrayValue" in value:
        return [any_value(item) for item in value["arrayValue"].get("values", [])]
    if "kvlistValue" in value:
        return attributes(value["kvlistValue"].get("values", []))
    return None


def attributes(entries):
    return {entry["key"]: any_value(entry.get("value")) for entry in entries}


def execution_records(record, state):
    if not isinstance(record, dict) or not OTLP_KEYS.intersection(record):
        yield record
        return
    state["otlp"] = True
    for resource in record.get("resourceLogs", []):
        resource_attributes = attributes(resource.get("resource", {}).get("attributes", []))
        for scope in resource.get("scopeLogs", []):
            logs = scope.get("logRecords", [])
            if scope.get("scope", {}).get("name") != EXECUTION_SCOPE:
                state["ignoredOther"] += len(logs)
                continue
            if resource_attributes.get("strands.box.source") != "box":
                state["ignoredUntrusted"] += len(logs)
                continue
            run_id = resource_attributes.get("strands.box.run.id")
            if not isinstance(run_id, str) or not run_id:
                raise ValueError("Execution telemetry is missing strands.box.run.id")
            for log in logs:
                version = attributes(log.get("attributes", [])).get("strands.box.execution.version")
                if version not in (3, "3"):
                    raise ValueError(f"Unsupported execution telemetry version: {version}")
                body = any_value(log.get("body"))
                event = json.loads(body) if isinstance(body, str) else body
                if not isinstance(event, dict) or not event.get("kind"):
                    raise ValueError(f"Invalid execution event payload in run {run_id}")
                if event["kind"] not in METADATA_KINDS and isinstance(event.get("sequence"), int):
                    seen = state["seen"].setdefault(run_id, set())
                    if event["sequence"] in seen:
                        state["duplicates"] += 1
                        continue
                    seen.add(event["sequence"])
                event["run_id"] = run_id
                event["box_name"] = resource_attributes.get("strands.box.name", "Box")
                event["execution_version"] = 3
                state["execution"] += 1
                yield event


def trailing_records(incoming, state):
    pending = None
    for line in incoming:
        if not line.strip():
            continue
        if pending is not None:
            yield json.loads(pending)
        pending = line
    if pending is not None:
        try:
            yield json.loads(pending)
        except json.JSONDecodeError:
            if pending.endswith("\n"):
                raise
            state["truncated"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--name", default="Box execution")
    parser.add_argument("--source", choices=("recorded", "synthetic"))
    parser.add_argument("--format", choices=("ndjson", "json"), default="ndjson")
    parser.add_argument("--template", type=Path, default=Path(__file__).with_name("viewer.html"))
    args = parser.parse_args()
    template = args.template.read_text()
    marker = r'(<script\b[^>]*\bid=["\']trace-data["\'][^>]*>).*?(</script>)'
    matches = list(re.finditer(marker, template, flags=re.S))
    if len(matches) != 1:
        parser.error('The template must contain one script with id="trace-data".')
    match = matches[0]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    state = {"otlp": False, "execution": 0, "ignoredOther": 0,
             "ignoredUntrusted": 0, "duplicates": 0, "seen": {}, "truncated": False}
    try:
        with args.trace.open() as incoming:
            first_line = next((line for line in incoming if line.strip()), "")
            if not first_line:
                parser.error("The trace has no records.")
            try:
                first = json.loads(first_line)
            except json.JSONDecodeError:
                first = None
            source = args.source or "recorded"
            metadata = {}
            if isinstance(first, dict) and "events" not in first and ("kind" in first or OTLP_KEYS.intersection(first)):
                if first.get("kind") == "trace_metadata":
                    metadata = {key: value for key, value in first.items() if key != "kind"}
                    source = args.source or first.get("source", "recorded")
                    initial = ()
                else:
                    initial = (first,)
                records = itertools.chain(initial, trailing_records(incoming, state))
            else:
                incoming.seek(0)
                data = json.load(incoming)
                if isinstance(data, dict) and OTLP_KEYS.intersection(data):
                    records = (data,)
                elif isinstance(data, dict):
                    metadata = {key: value for key, value in data.items() if key not in ("events", "kind")}
                    records = data.get("events", [])
                    source = args.source or data.get("source", "recorded")
                else:
                    records = data
            count = 0
            with tempfile.NamedTemporaryFile(mode="w", dir=args.output.parent, suffix=".html.tmp", delete=False) as output:
                temporary = Path(output.name)
                script_tag = re.sub(r'\btype=["\'][^"\']*["\']',
                                    'type="application/x-ndjson"' if args.format == "ndjson" else 'type="application/json"',
                                    match.group(1))
                output.write(template[:match.start(1)] + script_tag)
                metadata.update(name=args.name, source=source)
                output.write(encoded({"kind": "trace_metadata", **metadata}) + "\n" if args.format == "ndjson"
                             else encoded(metadata)[:-1] + ',"events":[')
                for record in records:
                    for event in execution_records(record, state):
                        if count and args.format == "json":
                            output.write(",")
                        output.write(encoded(event))
                        if args.format == "ndjson":
                            output.write("\n")
                        count += 1
                if state["otlp"] or state["truncated"]:
                    summary = {"kind": "trace_metadata", "truncated_final_line": state["truncated"],
                               "_wireStats": {key: value for key, value in state.items() if key not in ("seen", "truncated")}}
                    if count and args.format == "json":
                        output.write(",")
                    output.write(encoded(summary))
                    if args.format == "ndjson":
                        output.write("\n")
                if args.format == "json":
                    output.write("]}")
                output.write(template[match.start(2):])
        temporary.replace(args.output)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    print(f"Playback: {args.output.resolve()} ({count} {source} execution records)")


if __name__ == "__main__":
    main()
