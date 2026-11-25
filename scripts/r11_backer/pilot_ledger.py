"""A run-scoped GitHub ledger for public, synthetic integration state only."""

import base64
import json
import os
from random import uniform
import subprocess
from time import sleep, time

from .quota import can_reserve, retry_deadline


class PilotFailure(RuntimeError):
    """A fixed diagnostic label safe for the public workflow log."""


def require(condition, label):
    if not condition:
        raise PilotFailure(label)


class PilotLedger:
    def __init__(self):
        run, attempt = (int(os.environ[key]) for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"))
        self.branch = f"codex/r11-integration-{run}-{attempt}"
        self.api = f"repos/{os.environ['GITHUB_REPOSITORY']}"
        self.path = f"{self.api}/contents/.jarvis-test/r11-integration.json"

    @staticmethod
    def api_call(*args):
        return subprocess.run(["gh", "api", *args], capture_output=True, text=True, check=False)

    def read(self):
        result = self.api_call(f"{self.path}?ref={self.branch}")
        require(result.returncode == 0, "LEDGER_READ")
        document = json.loads(result.stdout)
        return document["sha"], json.loads(base64.b64decode(document["content"]))

    def write(self, state, expected=None):
        encoded = base64.b64encode(json.dumps(state, sort_keys=True).encode()).decode()
        args = ["--method", "PUT", self.path, "-f", f"branch={self.branch}",
                "-f", f"content={encoded}", "-f", "message=Update synthetic R11 integration state"]
        if expected is not None:
            args += ["-f", f"sha={expected}"]
        result = self.api_call(*args)
        if result.returncode == 0:
            return True
        if expected and "HTTP 409" in result.stderr:
            return False
        if expected and "HTTP 422" in result.stderr and self.read()[0] != expected:
            return False
        require(False, "LEDGER_WRITE")

    def seed(self, manifest, tasks, held):
        result = self.api_call("--method", "POST", f"{self.api}/git/refs",
                               "-f", f"ref=refs/heads/{self.branch}", "-f", f"sha={os.environ['GITHUB_SHA']}")
        require(result.returncode == 0, "LEDGER_CREATE_REF")
        self.write({
            "revision": 0, "manifest": manifest,
            "tasks": {key: {"status": "PENDING", "generation": 0} for key in tasks} | {
                held: {"status": "HOLD", "generation": 0, "reason": "G0_MISSING_COVERAGE"},
            },
            "folders": {}, "events": [], "paused_until": 0,
        })

    def update(self, change):
        for _ in range(20):
            sha, state = self.read()
            result = change(state)
            if result is None:
                return None
            # The revision keeps the content SHA unique even if values otherwise repeat.
            state["revision"] += 1
            if self.write(state, sha):
                return result
            sleep(uniform(0.1, 0.8))
        raise PilotFailure("LEDGER_CONTENTION")

    def claim(self, key, owner, *, recovery=False):
        def reserve(state):
            now = int(time())
            task = state["tasks"][key]
            if recovery:
                if task["status"] != "STAGED" or task["deadline"] > now:
                    return None
            else:
                if task["status"] != "PENDING":
                    return None
                if not can_reserve(state["events"], now, "download", paused_until=state["paused_until"]):
                    return None
                state["events"].append([now, "download"])
            task.update(status="LEASED", generation=task["generation"] + 1, owner=owner, deadline=now + 180)
            return task["generation"]

        return self.update(reserve)

    def finalize(self, key, owner, generation, *, interrupt=False):
        def commit(state):
            now = int(time())
            task = state["tasks"][key]
            if (task["status"] != "LEASED" or task["generation"] != generation
                    or task["owner"] != owner or task["deadline"] <= now):
                return None
            if interrupt:
                # Model a runner disappearing after the archive is durable.
                task.update(status="STAGED", deadline=now)
            else:
                task["status"] = "PASS"
            return True

        return self.update(commit) is True

    def simulate_retry_after(self):
        def pause(state):
            now = int(time())
            deadline = retry_deadline("180", now)
            state["paused_until"] = max(state["paused_until"], deadline)
            return deadline

        return self.update(pause)

    def reserve_folder(self, key, owner):
        def reserve(state):
            # An uncertain create is read back or held. A second worker never creates it.
            if key in state["folders"]:
                return None
            state["folders"][key] = {"status": "CREATING", "owner": owner}
            return True

        return self.update(reserve) is True

    def folder_ready(self, key, owner):
        def ready(state):
            folder = state["folders"][key]
            require(folder == {"status": "CREATING", "owner": owner}, "FOLDER_OWNER")
            folder["status"] = "READY"
            return True

        self.update(ready)
