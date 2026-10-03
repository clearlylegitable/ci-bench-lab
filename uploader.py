import json
import os
import sys

os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")

protocol = sys.stdout
sys.stdout = sys.stderr

from huggingface_hub import HfApi

apis = {}


def api_for(token):
    api = apis.get(token)
    if api is None:
        api = HfApi(token=token)
        apis[token] = api
    return api


def bucket_id_of(repo):
    if repo.startswith("buckets/"):
        return repo[len("buckets/"):]
    raise ValueError("not a bucket repo: " + repo)


def reply(message):
    protocol.write(json.dumps(message) + "\n")
    protocol.flush()


for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue

    request_id = -1
    try:
        request = json.loads(line)
        request_id = request.get("id", -1)
        add = [(item["tempPath"], item["path"]) for item in request["items"]]
        api_for(request["token"]).batch_bucket_files(bucket_id_of(request["repo"]), add=add)
        reply({"id": request_id, "ok": True})
    except Exception as exc:
        reply({"id": request_id, "ok": False, "error": type(exc).__name__ + ": " + str(exc)})
