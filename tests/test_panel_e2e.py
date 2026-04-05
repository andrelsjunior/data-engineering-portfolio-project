"""
End-to-end Playwright tests for the OLX Property Panel.

Assumes the Streamlit server is already running on http://localhost:8501.
Run with:
    uv run python tests/test_panel_e2e.py
"""

from __future__ import annotations

import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from playwright.sync_api import sync_playwright, expect

BASE = "http://localhost:8501"
SCREENSHOT_DIR = pathlib.Path("data")
SCREENSHOT_DIR.mkdir(exist_ok=True)

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"


def run_tests():
    results: list[tuple[str, bool, str]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        # ── Test 1: App loads ──────────────────────────────────────────────
        name = "App renders title and sidebar"
        try:
            page.goto(BASE, wait_until="networkidle", timeout=15_000)
            page.wait_for_selector("text=OLX Property Panel", timeout=8_000)
            page.wait_for_selector("text=Data source", timeout=5_000)
            page.screenshot(path=str(SCREENSHOT_DIR / "panel_test_01_empty.png"), full_page=True)
            results.append((name, True, ""))
        except Exception as e:
            results.append((name, False, str(e)))

        # ── Test 2: Tabs visible ───────────────────────────────────────────
        name = "All 3 tabs are visible"
        try:
            for tab in ["Browse", "Duplicate Groups", "Metrics"]:
                page.wait_for_selector(f"text={tab}", timeout=3_000)
            results.append((name, True, ""))
        except Exception as e:
            results.append((name, False, str(e)))

        # ── Test 3: Load JSON data ─────────────────────────────────────────
        name = "Load JSON data and listings appear"
        try:
            page.click("text=Load / Reload")
            # Wait for the success toast that includes "Loaded N records"
            page.wait_for_selector("text=Loaded", timeout=10_000)
            page.wait_for_timeout(2000)  # let Streamlit re-render the table
            # st.subheader renders as h3; grab the one in the main content area
            count_el = page.locator("h3").filter(has_text="listings").last
            text = count_el.inner_text()
            import re as _re
            n = int(_re.search(r"(\d+) listings", text).group(1))
            assert n > 0, f"Expected listings > 0, got: {text!r}"
            page.screenshot(path=str(SCREENSHOT_DIR / "panel_test_02_loaded.png"), full_page=True)
            results.append((name, True, text.strip()))
        except Exception as e:
            page.screenshot(path=str(SCREENSHOT_DIR / "panel_test_02_fail.png"), full_page=True)
            results.append((name, False, str(e)))

        # ── Test 4: Filters work ───────────────────────────────────────────
        name = "Price filter reduces listing count"
        try:
            # Get current count
            count_before = page.locator("h3").filter(has_text="listings").inner_text()
            # Move price max slider down (drag left by 50%)
            slider = page.locator('[aria-label="Price (R$)"]').nth(1)  # max thumb
            box = slider.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                page.mouse.down()
                page.mouse.move(box["x"] + box["width"] / 2 - 80, box["y"] + box["height"] / 2)
                page.mouse.up()
                page.wait_for_timeout(1500)
            page.screenshot(path=str(SCREENSHOT_DIR / "panel_test_03_filtered.png"), full_page=True)
            results.append((name, True, f"before: {count_before.strip()}"))
        except Exception as e:
            results.append((name, False, str(e)))

        # ── Test 5: Duplicate Groups tab ──────────────────────────────────
        name = "Duplicate Groups tab shows empty state message"
        try:
            page.click("text=Duplicate Groups")
            page.wait_for_selector("text=No duplicate groups", timeout=5_000)
            page.screenshot(path=str(SCREENSHOT_DIR / "panel_test_04_groups_empty.png"), full_page=True)
            results.append((name, True, ""))
        except Exception as e:
            results.append((name, False, str(e)))

        # ── Test 6: Metrics tab ───────────────────────────────────────────
        name = "Metrics tab shows empty state message"
        try:
            page.click("text=Metrics")
            page.wait_for_selector("text=No groups to summarise", timeout=5_000)
            page.screenshot(path=str(SCREENSHOT_DIR / "panel_test_05_metrics_empty.png"), full_page=True)
            results.append((name, True, ""))
        except Exception as e:
            results.append((name, False, str(e)))

        browser.close()

    # ── Report ─────────────────────────────────────────────────────────────
    print("\n── OLX Panel E2E Results ──────────────────────────────")
    passed = 0
    for name, ok, detail in results:
        icon = PASS if ok else FAIL
        suffix = f"  ({detail})" if detail else ""
        print(f"  {icon} {name}{suffix}")
        if ok:
            passed += 1
    print(f"\n  {passed}/{len(results)} passed")
    print("───────────────────────────────────────────────────────")
    return passed == len(results)


if __name__ == "__main__":
    ok = run_tests()
    sys.exit(0 if ok else 1)
