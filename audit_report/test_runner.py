import os
import glob
import subprocess
import sys
import time

test_files = sorted(glob.glob("backend/tests/test_*.py"))
print(f"Total backend test files found: {len(test_files)}")

results = []
for tf in test_files:
    sys.stdout.flush()
    t0 = time.time()
    try:
        proc = subprocess.run(
            [os.path.abspath("backend/venv/Scripts/python.exe"), "-m", "pytest", tf, "-q"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=15,
            cwd=os.path.abspath(".")
        )
        dur = time.time() - t0
        output = proc.stdout.strip()
        last_line = output.split("\n")[-1] if output else "NO OUTPUT"
        code = proc.returncode
        status = "PASS" if code == 0 else f"FAIL(code={code})"
        print(f"[{status}] {tf} ({dur:.2f}s): {last_line}")
        results.append((tf, status, dur, last_line))
    except subprocess.TimeoutExpired:
        dur = time.time() - t0
        print(f"[TIMEOUT] {tf} (>15s)")
        results.append((tf, "TIMEOUT", dur, "TimeoutExpired >15s"))
    except Exception as e:
        dur = time.time() - t0
        print(f"[ERROR] {tf}: {e}")
        results.append((tf, "ERROR", dur, str(e)))

print("\n" + "="*50)
print("TEST SUMMARY")
print("="*50)
passes = sum(1 for r in results if r[1] == "PASS")
fails = sum(1 for r in results if "FAIL" in r[1])
timeouts = sum(1 for r in results if r[1] == "TIMEOUT")
print(f"Pass: {passes}, Fail: {fails}, Timeout: {timeouts}, Total: {len(results)}")
for r in results:
    if r[1] != "PASS":
        print(f"  {r[0]} -> {r[1]} ({r[3]})")
