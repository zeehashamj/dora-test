#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


def parse_iso(ts: str):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def duration_seconds(start_ts: str, end_ts: str):
    start = parse_iso(start_ts)
    end = parse_iso(end_ts)
    if not start or not end:
        return ""
    return max(0, int((end - start).total_seconds()))


def fetch_commit_timestamp(repo: str, sha: str, token: str):
    if not repo or not sha:
        return ""
    url = f"https://api.github.com/repos/{repo}/commits/{sha}"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("commit", {}).get("committer", {}).get("date", "")
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return ""


def parse_bool(val: str):
    return str(val).lower() in ("1", "true", "yes", "y")


def main():
    parser = argparse.ArgumentParser(description="Build DORA payload")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--is-production", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--run-number", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--ref-name", required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--run-started-at", required=True)
    parser.add_argument("--completed-at", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--github-token", default="")
    args = parser.parse_args()

    commit_timestamp = fetch_commit_timestamp(args.repo, args.sha, args.github_token)
    deployment_duration = duration_seconds(args.run_started_at, args.completed_at)
    lead_time = duration_seconds(commit_timestamp, args.completed_at)
    change_failure_candidate = args.status != "success"

    payload = {
        "event_type": "deployment_finished",
        "repo": args.repo,
        "environment": args.environment,
        "is_production": parse_bool(args.is_production),
        "status": args.status,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "run_number": args.run_number,
        "workflow": args.workflow,
        "job": args.job,
        "actor": args.actor,
        "ref_name": args.ref_name,
        "sha": args.sha,
        "run_started_at": args.run_started_at,
        "completed_at": args.completed_at,
        "run_url": args.run_url,
        "commit_timestamp": commit_timestamp,
        "deployment_duration_seconds": deployment_duration,
        "lead_time_seconds_from_commit": lead_time,
        "change_failure_candidate": change_failure_candidate,
    }

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
