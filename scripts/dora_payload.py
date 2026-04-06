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


def request_json(url: str, token: str):
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None


def fetch_commit_timestamp(repo: str, sha: str, token: str):
    if not repo or not sha:
        return ""
    data = request_json(f"https://api.github.com/repos/{repo}/commits/{sha}", token)
    if not isinstance(data, dict):
        return ""
    return data.get("commit", {}).get("committer", {}).get("date", "")


def fetch_associated_pr(repo: str, sha: str, token: str):
    if not repo or not sha:
        return None
    data = request_json(f"https://api.github.com/repos/{repo}/commits/{sha}/pulls", token)
    if isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def fetch_first_commit_timestamp(repo: str, pr_number: int, token: str):
    data = request_json(f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits?per_page=1", token)
    if isinstance(data, list) and len(data) > 0:
        return data[0].get("commit", {}).get("committer", {}).get("date", "")
    return ""


def parse_bool(val: str):
    return str(val).lower() in ("1", "true", "yes", "y")


def format_prometheus(payload: dict) -> str:
    labels = ",".join(
        f'{k}="{v}"'
        for k, v in {
            "repo": payload.get("repo", ""),
            "environment": payload.get("environment", ""),
            "ref_name": payload.get("ref_name", ""),
            "workflow": payload.get("workflow", ""),
            "job": payload.get("job", ""),
            "actor": payload.get("actor", ""),
            "sha": payload.get("sha", ""),
            "status": payload.get("status", ""),
            "run_id": payload.get("run_id", ""),
        }.items()
    )

    def metric(name, help_text, value, mtype="gauge"):
        if value == "" or value is None:
            return ""
        return (
            f"# HELP {name} {help_text}\n"
            f"# TYPE {name} {mtype}\n"
            f"{name}{{{labels}}} {value}\n"
        )

    ts_epoch = ""
    if payload.get("completed_at"):
        completed = parse_iso(payload["completed_at"])
        if completed:
            ts_epoch = int(completed.timestamp())

    lines = [
        metric("dora_deployment_duration_seconds",
               "Duration of the deployment in seconds",
               payload.get("deployment_duration_seconds")),
        metric("dora_lead_time_from_commit_seconds",
               "Lead time from commit to deployment completion",
               payload.get("lead_time_seconds_from_commit")),
        metric("dora_lead_time_from_pr_open_seconds",
               "Lead time from PR open to deployment completion",
               payload.get("lead_time_from_pr_open")),
        metric("dora_lead_time_from_pr_merge_seconds",
               "Lead time from PR merge to deployment completion",
               payload.get("lead_time_from_pr_merge")),
        metric("dora_lead_time_from_first_commit_seconds",
               "Lead time from first commit in PR to deployment completion",
               payload.get("lead_time_from_first_commit")),
        metric("dora_change_failure",
               "Whether this deployment is a change failure candidate (1=yes 0=no)",
               1 if payload.get("change_failure_candidate") else 0),
        metric("dora_deployment_timestamp_seconds",
               "Unix timestamp of deployment completion",
               ts_epoch),
    ]

    return "\n".join(line for line in lines if line)


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
    parser.add_argument("--prometheus", action="store_true",
                        help="Output metrics in Prometheus exposition format")
    args = parser.parse_args()

    commit_timestamp = fetch_commit_timestamp(args.repo, args.sha, args.github_token)
    deployment_duration = duration_seconds(args.run_started_at, args.completed_at)
    lead_time = duration_seconds(commit_timestamp, args.completed_at)
    change_failure_candidate = args.status != "success"

    pr = fetch_associated_pr(args.repo, args.sha, args.github_token)
    pr_number = ""
    pr_created_at = ""
    pr_merged_at = ""
    pr_first_commit_at = ""

    if isinstance(pr, dict):
        pr_number = pr.get("number", "")
        pr_created_at = pr.get("created_at", "") or ""
        pr_merged_at = pr.get("merged_at", "") or ""
        if pr_number:
            pr_first_commit_at = fetch_first_commit_timestamp(args.repo, pr_number, args.github_token)

    lead_time_from_pr_open = duration_seconds(pr_created_at, args.completed_at)
    lead_time_from_pr_merge = duration_seconds(pr_merged_at, args.completed_at)
    lead_time_from_first_commit = duration_seconds(pr_first_commit_at, args.completed_at)

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
        "pr_number": pr_number,
        "pr_created_at": pr_created_at,
        "pr_merged_at": pr_merged_at,
        "pr_first_commit_at": pr_first_commit_at,
        "lead_time_from_pr_open": lead_time_from_pr_open,
        "lead_time_from_pr_merge": lead_time_from_pr_merge,
        "lead_time_from_first_commit": lead_time_from_first_commit,
        "change_failure_candidate": change_failure_candidate,
    }

    if args.prometheus:
        print(format_prometheus(payload))
    else:
        print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
