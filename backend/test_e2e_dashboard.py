"""
End-to-end test: Upload CSV -> Profile -> AI Dashboard prompt
Tests the full production flow programmatically.
"""
import sys
import json
import asyncio
import httpx
import time

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE = "http://localhost:8000"
API_KEY = "change-me-to-a-secure-secret"  # from .env DATA_INTEL_API_KEY
HEADERS = {"X-API-Key": API_KEY}
CSV_PATH = "test_data.csv"

async def main():
    async with httpx.AsyncClient(timeout=60.0) as client:
        
        # ─── Step 1: Upload CSV ───────────────────────────────────
        print("=" * 60)
        print("[1] Uploading test_data.csv...")
        with open(CSV_PATH, "rb") as f:
            resp = await client.post(
                f"{BASE}/api/upload",
                files={"files": ("test_data.csv", f, "text/csv")},
                headers=HEADERS,
            )
        
        if resp.status_code != 200:
            print(f"  FAIL: Upload returned {resp.status_code}")
            print(f"  Body: {resp.text[:300]}")
            return
        
        upload_data = resp.json()
        file_id = upload_data.get("file_id")
        success = upload_data.get("success", False)
        row_count = upload_data.get("metadata", {}).get("row_count", "?")
        col_count = upload_data.get("metadata", {}).get("col_count", "?")
        
        print(f"  Success: {success}")
        print(f"  File ID: {file_id}")
        print(f"  Rows: {row_count}, Cols: {col_count}")
        
        if not success or not file_id:
            print("  FAIL: Upload was not successful")
            return
        print("  PASS")
        
        # ─── Step 2: Get Profile ──────────────────────────────────
        print(f"\n[2] Fetching profile for {file_id}...")
        resp = await client.get(
            f"{BASE}/api/profile/{file_id}",
            headers=HEADERS,
        )
        
        if resp.status_code == 200:
            profile = resp.json()
            cols = profile.get("profile", {}).get("columns", [])
            col_names = [c.get("name", "?") for c in cols]
            print(f"  Columns: {col_names}")
            print(f"  PASS")
        else:
            print(f"  WARN: Profile returned {resp.status_code}: {resp.text[:200]}")
        
        # Step 3 was removed since it is done automatically during upload.

        # ─── Step 4: Test AI Dashboard - Interpret prompt ─────────
        print(f"\n[4] Testing AI Dashboard interpret endpoint...")
        print(f"  Prompt: 'Show me average salary by department as a bar chart'")
        
        resp = await client.post(
            f"{BASE}/api/dashboard/interpret",
            json={
                "file_id": file_id,
                "message": "Show me average salary by department as a bar chart",
            },
            headers=HEADERS,
        )
        
        if resp.status_code != 200:
            print(f"  FAIL: Dashboard interpret returned {resp.status_code}")
            print(f"  Body: {resp.text[:500]}")
        else:
            dash_data = resp.json()
            dash_success = dash_data.get("success", False)
            config = dash_data.get("config", {})
            data = dash_data.get("data", [])
            error = dash_data.get("error")
            
            print(f"  Success: {dash_success}")
            if error:
                print(f"  Error: {error}")
            if config:
                print(f"  Chart type: {config.get('chart_type', '?')}")
                print(f"  Title: {config.get('title', '?')}")
                print(f"  X column: {config.get('x_column', '?')}")
                print(f"  Y column: {config.get('y_column', '?')}")
                sql = config.get("sql", config.get("query", "?"))
                print(f"  SQL: {str(sql)[:200]}")
            if data:
                print(f"  Data rows returned: {len(data)}")
                print(f"  Sample: {json.dumps(data[:2], indent=2)[:300]}")
            else:
                print(f"  Data: (empty)")
            
            if dash_success and config:
                print(f"  PASS")
            else:
                print(f"  FAIL")
        
        # ─── Step 5: Test a second dashboard prompt ───────────────
        print(f"\n[5] Testing second prompt: 'scatter plot of age vs salary'...")
        
        resp = await client.post(
            f"{BASE}/api/dashboard/interpret",
            json={
                "file_id": file_id,
                "message": "scatter plot of age vs salary colored by department",
            },
            headers=HEADERS,
        )
        
        if resp.status_code != 200:
            print(f"  FAIL: returned {resp.status_code}")
            print(f"  Body: {resp.text[:500]}")
        else:
            dash_data = resp.json()
            dash_success = dash_data.get("success", False)
            config = dash_data.get("config", {})
            data = dash_data.get("data", [])
            error = dash_data.get("error")
            
            print(f"  Success: {dash_success}")
            if error:
                print(f"  Error: {error}")
            if config:
                print(f"  Chart type: {config.get('chart_type', '?')}")
                print(f"  Title: {config.get('title', '?')}")
            if data:
                print(f"  Data rows: {len(data)}")
            
            if dash_success and config:
                print(f"  PASS")
            else:
                print(f"  FAIL")
        
        # ─── Summary ─────────────────────────────────────────────
        print(f"\n{'=' * 60}")
        print(f"END-TO-END TEST COMPLETE")
        print(f"{'=' * 60}")

asyncio.run(main())
