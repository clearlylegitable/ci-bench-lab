import json
import os
from huggingface_hub import HfApi

uploaded = json.load(open("/tmp/uploaded.json")) if os.path.exists("/tmp/uploaded.json") else {}
tokens = {os.environ["HF_REPO_1"]: os.environ["HF_TOKEN_1"], os.environ["HF_REPO_2"]: os.environ["HF_TOKEN_2"]}

for repo, names in uploaded.items():
    token = tokens.get(repo)
    if not token or not names:
        continue
    api = HfApi(token=token)
    bucket_id = repo.split("/", 1)[1]
    for i in range(0, len(names), 200):
        api.batch_bucket_files(bucket_id, delete=names[i:i + 200])
    print("deleted", len(names), "from", repo)
