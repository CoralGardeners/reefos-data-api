"""Generate the REST API section of README.md from ep.api_help().

Usage:
    python scripts/update_readme.py

Run this after adding or modifying endpoints in endpoints.py.
The script rewrites the content between the
<!-- BEGIN_REST_API --> and <!-- END_REST_API --> markers in README.md.
"""

import json
import re
import sys
from pathlib import Path

# Allow running from the repo root or from scripts/
sys.path.insert(0, str(Path(__file__).parent.parent))

import reefos_data_api.endpoints as ep

README = Path(__file__).parent.parent / "README.md"
BEGIN_MARKER = "<!-- BEGIN_REST_API -->"
END_MARKER = "<!-- END_REST_API -->"


def _endpoint_rows(endpoints: dict) -> list[str]:
    rows = []
    for name, info in endpoints.items():
        desc = info.get("description", "")
        # Some endpoints have a single url, others have a urls sub-dict
        url = info.get("url", "")
        if not url:
            urls = info.get("urls", {})
            url = ", ".join(f"`{v}`" for v in urls.values()) if urls else "—"
        else:
            url = f"`{url}`"

        params = info.get("params", info.get("common_params", {}))
        if params:
            param_str = "<br>".join(
                f"`{k}`: {v}" for k, v in params.items() if k != "pwd"
            )
        else:
            param_str = "—"

        rows.append(f"| `{name}` | {url} | {desc} | {param_str} |")
    return rows


def generate_section(help_data: dict) -> str:
    endpoints = help_data.get("endpoints", {})
    base_url = help_data.get("base_url", "")
    auth = help_data.get("auth", "")

    lines = [
        f"Base URL: `{base_url}`  ",
        f"Auth: {auth}",
        "",
        "| Endpoint | URL | Description | Parameters |",
        "|----------|-----|-------------|------------|",
    ]
    lines.extend(_endpoint_rows(endpoints))
    return "\n".join(lines)


def update_readme(section: str) -> None:
    text = README.read_text()
    pattern = re.compile(
        rf"{re.escape(BEGIN_MARKER)}.*?{re.escape(END_MARKER)}",
        re.DOTALL,
    )
    replacement = f"{BEGIN_MARKER}\n{section}\n{END_MARKER}"
    new_text, count = pattern.subn(replacement, text)
    if count == 0:
        print("ERROR: markers not found in README.md — add them first.", file=sys.stderr)
        sys.exit(1)
    README.write_text(new_text)
    print(f"README.md updated ({len(section.splitlines())} lines in REST API section).")


if __name__ == "__main__":
    help_data = json.loads(ep.api_help())
    section = generate_section(help_data)
    update_readme(section)
