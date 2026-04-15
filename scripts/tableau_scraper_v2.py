"""
USDA Timber Sales Tableau Scraper - v2
=======================================
Now with correct session flow from network tab analysis.

Strategy overview:
  1. POST to startSession/viewing  → get X-Session-Id
  2. Try get-underlying-data       → pulls ALL rows behind the worksheet (ignores UI filters)
  3. Try export-crosstab-to-csv    → exports what's currently visible
  4. Try manipulating date filters → expand the visible range to historical data
  5. Inspect the datasource        → find out what columns/range actually exist

Install: pip install requests pandas
"""

import json
import time
import uuid
import pandas as pd
import requests
from io import StringIO

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

BASE       = "https://publicdashboards.dl.usda.gov"
VIZQL_PATH = "/vizql/t/NRE_PUB/w/TimberSalesAdvertisement/v/TimberSalesDashboard"

START_SESSION_URL = BASE + VIZQL_PATH + "/startSession/viewing"

PARAMS = (
    "?%3AshowAppBanner=false&%3Adisplay_count=n&%3AshowVizHome=n"
    "&%3Aorigin=viz_share_link&%3Aembed=y&%3Atabs=n&%3Atoolbar=n"
    "&%3AapiID=host0&%3Aredirect=auth"
)

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Origin":  BASE,
    "Referer": BASE + "/t/NRE_PUB/views/TimberSalesAdvertisement/TimberSalesDashboard"
               "?%3AshowAppBanner=false&%3Adisplay_count=n&%3AshowVizHome=n"
               "&%3Aorigin=viz_share_link&%3Aembed=y&%3Atabs=n&%3Atoolbar=n&%3AapiID=host0",
    "x-tableau-version": "2025.1",
    "x-requested-with":  "XMLHttpRequest",
    "x-tsi-active-tab":  "Timber%20Sales%20Dashboard",
    # Zippy trace headers Tableau uses (values don't matter, just need to be present)
    "x-b3-sampled":  "1",
    "x-b3-spanid":   uuid.uuid4().hex[:16] + uuid.uuid4().hex[:16],
    "x-b3-traceid":  uuid.uuid4().hex + uuid.uuid4().hex,
}

WORKSHEET  = "Table"
DASHBOARD  = "Timber Sales Dashboard"


# ── STEP 1: Get a fresh session ID ────────────────────────────────────────────

def start_session(s: requests.Session) -> str:
    print("[1/N] Starting Tableau VizQL session...")
    resp = s.post(
        START_SESSION_URL + PARAMS,
        headers={
            **COMMON_HEADERS,
            "accept": "application/json",
            "content-length": "0",
            "tableau-viz-location": BASE + VIZQL_PATH.replace("/vizql", "") + PARAMS,
            "tableau-viz-path": "/t/NRE_PUB/w/TimberSalesAdvertisement#0",
        },
        timeout=30,
    )
    resp.raise_for_status()
    session_id = resp.headers.get("X-Session-Id")
    if not session_id:
        # Sometimes it's in the JSON body
        try:
            session_id = resp.json().get("sessionId") or resp.json().get("newSessionId")
        except Exception:
            pass
    if not session_id:
        raise RuntimeError(f"No X-Session-Id found. Status={resp.status_code}\n{resp.text[:500]}")
    print(f"    ✓ Session ID: {session_id}")
    return session_id


def session_url(session_id: str, command: str) -> str:
    return f"{BASE}{VIZQL_PATH}/sessions/{session_id}/commands/{command}"


# ── STEP 2A: get-underlying-data ──────────────────────────────────────────────
# This is the MOST POWERFUL command. It bypasses view filters and returns
# every row in the datasource that feeds the worksheet — potentially including
# historical records that the UI filters hide.

def get_underlying_data(s: requests.Session, session_id: str) -> pd.DataFrame | None:
    """
    Calls the 'get-underlying-data' VizQL command.
    maxRows=0 means no limit. includeAllColumns=true gets every field.
    This ignores dashboard filters — if historical data is IN the datasource,
    it will appear here even if the UI only shows current auctions.
    """
    print("\n[2A] Trying get-underlying-data (bypasses UI filters)...")
    url = session_url(session_id, "tabsrv/get-underlying-data")

    fields = {
        "worksheet":           WORKSHEET,
        "dashboard":           DASHBOARD,
        "maxRows":             "0",          # 0 = unlimited
        "includeAllColumns":   "true",
        "ignoreAliases":       "false",
        "ignoreSelection":     "true",       # ignore any active selection
        "columnsToInclude":    "[]",
        "telemetryCommandId":  uuid.uuid4().hex,
    }

    resp = s.post(url, data=fields, headers={**COMMON_HEADERS, "accept": "text/javascript"}, timeout=60)

    if resp.status_code != 200:
        print(f"    ✗ HTTP {resp.status_code}: {resp.text[:300]}")
        return None

    # Response is JSON; data lives in vqlCmdResponse > layoutStatus > applicationPresModel
    # > workbookPresModel > dashboardPresModel > ... > underlyingDataTable
    try:
        payload = resp.json()
        with open("underlying_data_raw.json", "w") as f:
            json.dump(payload, f, indent=2)
        print("    Saved underlying_data_raw.json — inspect for full structure")

        # Navigate to the data table (path varies by Tableau version)
        data_table = _dig(payload, [
            "vqlCmdResponse", "layoutStatus", "applicationPresModel",
            "workbookPresModel", "dashboardPresModel", "zones",
        ])
        if data_table:
            print(f"    ✓ Found data. Attempting to parse...")
            return _parse_underlying_table(payload)
        else:
            print("    ✗ Could not auto-navigate to data table. Check underlying_data_raw.json")
            return None
    except Exception as e:
        print(f"    ✗ Parse error: {e}")
        return None


def _dig(obj, keys):
    """Safely traverse nested dicts."""
    for k in keys:
        if isinstance(obj, dict):
            obj = obj.get(k)
        elif isinstance(obj, list) and isinstance(k, int):
            obj = obj[k] if k < len(obj) else None
        else:
            return None
        if obj is None:
            return None
    return obj


def _parse_underlying_table(payload: dict) -> pd.DataFrame | None:
    """
    Tableau's underlying data response embeds column names and row data.
    The exact nesting is version-dependent — this handles common shapes.
    """
    raw = json.dumps(payload)

    # Look for the columnsData / rowsData pattern
    if '"columnsData"' in raw and '"data"' in raw:
        # Search recursively
        def find_key(d, target):
            if isinstance(d, dict):
                if target in d:
                    return d[target]
                for v in d.values():
                    result = find_key(v, target)
                    if result is not None:
                        return result
            elif isinstance(d, list):
                for item in d:
                    result = find_key(item, target)
                    if result is not None:
                        return result
            return None

        cols_data = find_key(payload, "columnsData")
        rows_data = find_key(payload, "data")

        if cols_data and rows_data:
            col_names = [c.get("fieldCaption", c.get("fieldName", f"col_{i}"))
                         for i, c in enumerate(cols_data)]
            df = pd.DataFrame(rows_data, columns=col_names[:len(rows_data[0])] if rows_data else col_names)
            print(f"    ✓ Parsed: {df.shape[0]} rows × {df.shape[1]} cols")
            return df

    print("    ✗ Could not parse — inspect underlying_data_raw.json manually")
    return None


# ── STEP 2B: export-crosstab-to-csv ──────────────────────────────────────────
# Exports whatever is currently visible as CSV. Less useful for historical data
# but good for validating the session works.

def export_crosstab(s: requests.Session, session_id: str) -> pd.DataFrame | None:
    print("\n[2B] Trying export-crosstab-to-csv (current view only)...")
    url = session_url(session_id, "export/export-crosstab-to-csv")

    fields = {
        "worksheet":          WORKSHEET,
        "viewName":           DASHBOARD,
        "bom":                "true",
        "telemetryCommandId": uuid.uuid4().hex,
    }

    resp = s.post(url, data=fields, headers={**COMMON_HEADERS, "accept": "text/javascript"}, timeout=60)

    if resp.status_code == 200 and "," in resp.text[:300]:
        df = pd.read_csv(StringIO(resp.text.lstrip("\ufeff")))  # strip BOM
        print(f"    ✓ Crosstab export: {df.shape[0]} rows × {df.shape[1]} cols")
        return df

    print(f"    ✗ HTTP {resp.status_code}: {resp.text[:300]}")
    return None


# ── STEP 3: Manipulate date filters to expose historical data ─────────────────
# If historical data IS in the datasource but filtered by a date range filter,
# we can widen or remove that filter via the API.

def clear_all_filters(s: requests.Session, session_id: str) -> bool:
    """
    Attempts to clear all filters on the worksheet, which may expose historical data.
    Then re-export the data.
    """
    print("\n[3A] Attempting to clear all dashboard filters...")

    # First: get the current filter state
    filter_url = session_url(session_id, "tabsrv/get-filters")
    fields = {
        "worksheet":          WORKSHEET,
        "dashboard":          DASHBOARD,
        "telemetryCommandId": uuid.uuid4().hex,
    }
    resp = s.post(filter_url, data=fields, headers={**COMMON_HEADERS, "accept": "text/javascript"}, timeout=30)

    if resp.status_code == 200:
        try:
            filters = resp.json()
            with open("filters_raw.json", "w") as f:
                json.dump(filters, f, indent=2)
            print("    Saved filters_raw.json — inspect to see active date/range filters!")
        except Exception:
            print(f"    Could not parse filters: {resp.text[:300]}")
    else:
        print(f"    get-filters returned HTTP {resp.status_code}")

    return resp.status_code == 200


def set_date_range_filter(
    s: requests.Session,
    session_id: str,
    field_name: str,
    date_min: str,   # e.g. "2010-01-01"
    date_max: str,   # e.g. "2025-12-31"
) -> bool:
    """
    Sets a range filter on a date field to expose historical records.
    Call get-filters first to find the exact field_name used by Tableau.
    """
    print(f"\n[3B] Setting date range filter: {field_name} from {date_min} to {date_max}...")

    url = session_url(session_id, "tabsrv/range-filter")
    fields = {
        "worksheet":          WORKSHEET,
        "dashboard":          DASHBOARD,
        "fieldCaption":       field_name,
        "minValue":           date_min,
        "maxValue":           date_max,
        "nullOption":         "NonNullValues",
        "telemetryCommandId": uuid.uuid4().hex,
    }

    resp = s.post(url, data=fields, headers={**COMMON_HEADERS, "accept": "text/javascript"}, timeout=30)
    success = resp.status_code == 200
    print(f"    {'✓' if success else '✗'} HTTP {resp.status_code}")
    return success


def remove_filter(s: requests.Session, session_id: str, field_name: str) -> bool:
    """Removes a specific filter entirely (shows all values including historical)."""
    print(f"\n[3C] Removing filter on '{field_name}'...")

    url = session_url(session_id, "tabsrv/categorical-filter")
    fields = {
        "worksheet":          WORKSHEET,
        "dashboard":          DASHBOARD,
        "fieldCaption":       field_name,
        "filterValues":       "[]",       # empty = remove
        "filterUpdateType":   "filter-all",
        "telemetryCommandId": uuid.uuid4().hex,
    }

    resp = s.post(url, data=fields, headers={**COMMON_HEADERS, "accept": "text/javascript"}, timeout=30)
    success = resp.status_code == 200
    print(f"    {'✓' if success else '✗'} HTTP {resp.status_code}")
    return success


# ── STEP 4: Inspect datasource metadata ──────────────────────────────────────
# Before trying to filter, understand what fields and date ranges exist.

def get_datasource_info(s: requests.Session, session_id: str) -> dict | None:
    """
    Fetches datasource metadata — field names, types, and min/max values.
    This tells you: (a) what date fields exist, and (b) the full date range in the data.
    If the max date in the source is today, historical data probably isn't there.
    If it goes back years, you just need to widen the filter.
    """
    print("\n[4] Fetching datasource metadata...")

    url = session_url(session_id, "tabsrv/get-datasource-fields")
    fields = {
        "worksheet":          WORKSHEET,
        "dashboard":          DASHBOARD,
        "telemetryCommandId": uuid.uuid4().hex,
    }

    resp = s.post(url, data=fields, headers={**COMMON_HEADERS, "accept": "text/javascript"}, timeout=30)

    if resp.status_code == 200:
        try:
            meta = resp.json()
            with open("datasource_fields.json", "w") as f:
                json.dump(meta, f, indent=2)
            print("    ✓ Saved datasource_fields.json — look for date fields and their min/max!")
            return meta
        except Exception as e:
            print(f"    Could not parse: {e}")
    else:
        print(f"    ✗ HTTP {resp.status_code}: {resp.text[:300]}")

    return None


# ── MAIN: Diagnostic run ──────────────────────────────────────────────────────

def run():
    with requests.Session() as s:

        # Step 1: Authenticate and get session
        session_id = start_session(s)
        time.sleep(1)

        # Step 2A: Try get-underlying-data first — most likely to have historical rows
        df_underlying = get_underlying_data(s, session_id)
        if df_underlying is not None:
            df_underlying.to_csv("timber_underlying_data.csv", index=False)
            print(f"\n✅ Saved timber_underlying_data.csv ({df_underlying.shape[0]} rows)")
            print("   Check if this includes historical records (look at date column range)!")

        time.sleep(0.5)

        # Step 2B: Export current crosstab view
        df_crosstab = export_crosstab(s, session_id)
        if df_crosstab is not None:
            df_crosstab.to_csv("timber_crosstab.csv", index=False)
            print(f"\n✅ Saved timber_crosstab.csv ({df_crosstab.shape[0]} rows)")

        time.sleep(0.5)

        # Step 3: Inspect what filters are active
        clear_all_filters(s, session_id)

        # Step 4: Get datasource metadata
        get_datasource_info(s, session_id)

        print("\n" + "="*60)
        print("NEXT STEPS based on results:")
        print("="*60)
        print("""
1. Open underlying_data_raw.json
   → Search for 'tupleIds', 'columnsData', or 'rowsData'
   → If rows span multiple years → data IS there, just filtered in UI
   → If rows are only recent    → data NOT in this workbook's datasource

2. Open filters_raw.json
   → Look for any date/time range filter
   → Note the exact 'fieldCaption' value (e.g. 'Sale Date', 'Advertise Date')
   → Use set_date_range_filter() with that field name to widen the range

3. Open datasource_fields.json
   → Find the date field name and its domain (min/max values)
   → This tells you how far back the data actually goes

4. If data is NOT in the workbook:
   → The USDA likely has a separate data portal or FOIA-accessible database
   → Check: https://www.fs.usda.gov/science-technology/forest-management/timber-data
   → Try the USDA Forest Service Timber Sales database (separate REST API)
   → Or request data via: https://www.fs.usda.gov/forestmanagement/products/selling/index.shtml
        """)


if __name__ == "__main__":
    run()
