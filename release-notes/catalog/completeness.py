"""Per-release no-loss invariant."""
import re

_LI_COUNT_RE = re.compile(r"<li\b", re.IGNORECASE)

def check_release(block_text: str, items) -> dict:
    total_li = len(_LI_COUNT_RE.findall(block_text))
    kept = sum(1 for it in items if it.disposition == "keep")
    dropped = sum(1 for it in items if it.disposition == "drop")
    unaccounted = total_li - (kept + dropped)
    return {
        "total_li": total_li,
        "kept": kept,
        "dropped": dropped,
        "unaccounted": unaccounted,
        "ok": unaccounted == 0,
    }
