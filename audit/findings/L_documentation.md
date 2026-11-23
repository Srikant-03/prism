# Dimension L: Documentation

Audit of README accuracy, setup instructions, API documentation, environment variable documentation, and contributing guides.

---

### Finding DOC-001: README Positions Single-User Windows Prototype as Enterprise Platform
- **ID**: DOC-001
- **Title**: Discrepancy Between README Marketing Claims and Technical Architecture
- **Type**: DOC
- **Severity**: P2
- **Location**: `README.md:1-250`
- **Evidence**:
  [README.md](file:///c:/abbu/README.md) advertises PRISM as a "Production-Grade Data Intelligence Platform" and enterprise alternative to Power BI / Tableau with "multi-user collaboration" and "autonomous agentic data analysis".
  In reality:
  - State is in-memory/diskcache with no user authentication or access controls.
  - Collaboration is an in-memory dictionary without WebSockets.
  - Deployment is a Windows `.bat` script.
- **Impact**: Sets incorrect expectations for engineering teams and stakeholders regarding security compliance, scalability, and multi-tenant capabilities.
- **Root cause**: Marketing copy written ahead of enterprise infrastructure implementation.
- **Recommended fix**: Update README to accurately state current status: "Local-first desktop data intelligence tool for individual analysts (single-user Windows/macOS/Linux prototype)". Add a clear production deployment disclaimer.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding DOC-002: Incomplete `.env.example` Omits Key Tuning Parameters
- **ID**: DOC-002
- **Title**: Configuration Variables Used in Code Missing from Environment Template
- **Type**: DOC
- **Severity**: P2
- **Location**: `backend/.env.example:1-45`, `backend/config.py:100-105`
- **Evidence**:
  Variables referenced in `config.py` and `ingestion/orchestrator.py`:
  - `INGESTION_STORE_MAX_ENTRIES`
  - `INGESTION_STORE_TTL_SECONDS`
  - `DUP_COL_SIMILARITY`
  - `DERIVED_R2_THRESHOLD`
  - `NEAR_DUP_SAMPLE`
  None of these appear in `backend/.env.example`.
- **Impact**: Operators cannot discover or adjust memory and detection thresholds without reading Python source code.
- **Root cause**: New configuration variables added to source code without updating the example env file.
- **Recommended fix**: Synchronize `backend/.env.example` to document all available tuning options with comments explaining units and defaults.
- **Confidence**: High
- **Label**: [VERIFIED]
