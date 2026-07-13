import json
from pathlib import Path
import yaml


def main(req):
    body = req.get_json()
    path = Path(body['yml_path'])
    with path.open('r', encoding='utf-8') as handle:
        config = yaml.safe_load(handle)
    return {
        'status_code': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(config)
    }
