"""
One-time interactive login for the TikTok Seller Center scraper.

Opens a headed Chromium, navigates to Seller Center, and waits for YOU to
complete the full login (email + password + 2FA). When the page becomes an
authenticated dashboard we persist the session's cookies + localStorage to
data/scraper/storage_state.json. scraper_server.py then boots with that
state and stays logged in for weeks.

USAGE
    python scripts/scraper_login.py

    # Optional: override the login URL if TikTok moves it
    SCRAPER_LOGIN_URL="https://seller-us.tiktok.com/account/login" \\
        python scripts/scraper_login.py

    # Optional: if you're on a headless machine (like OpenClaw) without a
    # display, set HEADLESS=1 and scan the QR code it prints -- OR run this
    # on a machine with a screen, save storage_state.json, then scp it to
    # the server. The scp path is way easier.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("Playwright is not installed. Run:")
    print("  pip install -r scripts/scraper_requirements.txt")
    print("  python -m playwright install chromium")
    sys.exit(1)


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
STATE_DIR = Path(os.environ.get("SCRAPER_STATE_DIR", PROJECT_ROOT / "data" / "scraper"))
STATE_DIR.mkdir(parents=True, exist_ok=True)
STORAGE_STATE_PATH = STATE_DIR / "storage_state.json"

LOGIN_URL = os.environ.get(
    "SCRAPER_LOGIN_URL",
    "https://seller-us.tiktok.com/account/login",
)
# Any URL that only appears AFTER login counts as success. Seller Center
# routes a logged-in user to /homepage (or similar); logged-out users stay
# under /account/login or /passport/.
SUCCESS_PATH_FRAGMENTS = ("/homepage", "/dashboard", "/order", "/product")
HEADLESS = os.environ.get("HEADLESS", "0").lower() in ("1", "true", "yes")


async def main() -> None:
    print(f"Login URL:          {LOGIN_URL}")
    print(f"Storage state path: {STORAGE_STATE_PATH}")
    print(f"Headless:           {HEADLESS}")
    print()
    print("A browser window will open. Log in (including 2FA / captcha).")
    print("Once the dashboard loads, this script will save your session")
    print("and exit automatically.")
    print()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(LOGIN_URL)

        print("Waiting for successful login...")
        print("(polling every 2s; Ctrl-C to abort)")

        while True:
            await asyncio.sleep(2)
            current = page.url
            if any(fragment in current for fragment in SUCCESS_PATH_FRAGMENTS):
                print(f"Detected authenticated URL: {current}")
                break

        await context.storage_state(path=str(STORAGE_STATE_PATH))
        print()
        print(f"Saved session to {STORAGE_STATE_PATH}")
        print("You can now start scripts/scraper_server.py.")

        await context.close()
        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(130)
