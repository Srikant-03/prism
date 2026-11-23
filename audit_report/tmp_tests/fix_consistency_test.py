import sys
import io

path = 'backend/tests/test_target_state_consistency.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Remove early TextIOWrapper
lines = content.splitlines(keepends=True)
clean_lines = []
for line in lines:
    if 'if hasattr(sys.stdout, "buffer"):' in line or 'if hasattr(sys.stderr, "buffer"):' in line or 'sys.stdout = io.TextIOWrapper' in line or 'sys.stderr = io.TextIOWrapper' in line:
        continue
    clean_lines.append(line)

content = ''.join(clean_lines)

sec1_marker = '# SECTION 1: TargetDetector heuristics'
pytest_marker = 'def test_target_state_consistency_all_sections():'

sec1_idx = content.find(sec1_marker)
pytest_idx = content.find(pytest_marker)

if sec1_idx == -1 or pytest_idx == -1:
    print(f"Error finding markers: sec1={sec1_idx}, pytest={pytest_idx}")
    sys.exit(1)

header = content[:sec1_idx]
body = content[sec1_idx:pytest_idx]

indented_body = '\n'.join(('    ' + l) if l.strip() else '' for l in body.splitlines())

runner = f'''def _run_all_sections():
{indented_body}

def test_target_state_consistency_all_sections():
    global PASS, FAIL, ERRORS
    PASS = 0
    FAIL = 0
    ERRORS = []
    _run_all_sections()
    assert FAIL == 0, f"Target state consistency had {{FAIL}} failures: {{ERRORS}}"
    assert PASS >= 70, f"Expected at least 70 checks to pass, got {{PASS}}"

if __name__ == "__main__":
    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer"):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    _run_all_sections()
    sys.exit(0 if FAIL == 0 else 1)
'''

with open(path, 'w', encoding='utf-8') as f:
    f.write(header + runner)

print("Successfully updated test_target_state_consistency.py")
