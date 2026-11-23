# Dimension K: Code Quality and Maintainability

Audit of structure, duplication, naming, dead code, complexity, coupling, type checking, and technical debt.

---

### Finding QUAL-001: Monolithic High-Complexity Hotspots
- **ID**: QUAL-001
- **Title**: Excessive File Length and Cyclomatic Complexity in Core Reporting and Story Modules
- **Type**: DEBT
- **Severity**: P2
- **Location**: `backend/reporting/report_generator.py:1-1248` (55.3 KB), `backend/api/story.py:1-950` (40.2 KB)
- **Evidence**:
  `report_generator.py` spans 1,248 lines handling PDF, DOCX, HTML, and Jupyter notebook rendering, CSS layout generation, and table styling in a single file with complex nested loops.
- **Impact**: Extremely high cognitive load for maintainers. Modifying PDF export logic risks introducing subtle regressions into DOCX or HTML pipelines. Unit testing individual document layout elements is virtually impossible without running the full monolithic generator.
- **Root cause**: Accumulation of multi-format export code in a single procedural module.
- **Recommended fix**: Refactor `report_generator.py` into format-specific submodules (`reporting/exporters/pdf.py`, `docx.py`, `html.py`, `notebook.py`) implementing a clean `BaseExporter` interface.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding QUAL-002: Proliferation of Abandoned Scratch Scripts in Backend Root
- **ID**: QUAL-002
- **Title**: 10+ Temporary Debug and Experimentation Scripts Cluttering Backend Root
- **Type**: DEBT
- **Severity**: P3
- **Location**: `backend/` root directory
- **Evidence**:
  Files present in `backend/`:
  - `diagnose_keys.py`
  - `patch_story.py`
  - `patch_target_state.py`
  - `run_test_local.py`
  - `test_gemini.py`, `test_gemini2.py`, `test_gemini3.py`, `test_gemini_drain.py`, `test_gemini_keys.py`
  - `test_tough_data.py`
  - `test_dashboard_full.py`
- **Impact**: Confuses developers regarding canonical test suites and build entry points. These files lack documentation, contain hardcoded test paths, and risk being imported accidentally into production paths.
- **Root cause**: Ad-hoc debugging scripts committed directly to repository root rather than placed in `scratch/` or removed after validation.
- **Recommended fix**: Archive or delete one-off debug scripts, moving legitimate integration tests into `backend/tests/` with proper pytest fixtures.
- **Confidence**: High
- **Label**: [VERIFIED]
