import json
import yaml
from pathlib import Path

src = Path("api/openapi/travel-safe-api-openapi.json")
dst = Path("api/openapi/travel-safe-api-openapi.yaml")

data = json.loads(src.read_text(encoding="utf-8"))
yaml_text = yaml.safe_dump(data, sort_keys=False)

dst.write_text(yaml_text, encoding="utf-8")
print(f"Wrote {dst}")
