#!/usr/bin/env bash
# Print the changelog for a single project since its previous release tag.
#
# Usage: release_message.sh [lexical-graph|byokg|both] [--to <ref>]
# Default project is "both". Delegates to scripts/generate_changelog.py, which
# classifies each commit by the project folders it touched. See RELEASE.md.
set -euo pipefail

project="${1:-both}"
shift || true

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${script_dir}/scripts/generate_changelog.py" --project "${project}" "$@"
