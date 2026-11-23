# Dimension N: Legal and Compliance

Audit of software licenses, asset licensing, privacy requirements (GDPR/CCPA/HIPAA), data residency, consent, and regulatory audit compliance.

---

### Finding LEG-001: Uninformed External Transmission of Sensitive Dataset Schemas to Cloud LLM
- **ID**: LEG-001
- **Title**: Column Names, Statistical Summaries, and Data Samples Sent to Google Gemini API Without Explicit Consent
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/sql/nl_query.py:75-105`, `backend/dashboard/prompt_interpreter.py:60-90`, `backend/chat/engine.py:50-80`
- **Evidence**:
  ```python
  # chat/engine.py
  # Injects dataset column names, sample rows, and summary stats into Gemini prompt
  prompt = f"Dataset Schema: {schema_summary}\nSample Data: {sample_rows}\nQuestion: {user_prompt}"
  response = client.models.generate_content(...)
  ```
- **Impact**: Organizations operating under strict compliance frameworks (GDPR in the EU, HIPAA for healthcare data, SOC 2, or confidential enterprise NDAs) assume local data processing based on PRISM's local-first marketing. Transmitting proprietary column headers and raw data samples to Google's public AI infrastructure violates data residency mandates and confidentiality obligations.
- **Root cause**: Lack of an explicit outbound AI data disclosure and opt-in consent toggle before initiating generative features.
- **Recommended fix**: Implement an upfront AI Data Disclosure dialog clearly stating what metadata and sample rows are transmitted to Google Gemini, with an option to operate in "Offline Mode" (using local Ollama or heuristics).
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding LEG-002: LGPL-3.0 License Compliance Review for `fpdf2` Dependency
- **ID**: LEG-002
- **Title**: Presence of LGPL-3.0 Dependency (`fpdf2`) Requires Proper Licensing Compliance
- **Type**: RISK
- **Severity**: P3
- **Location**: `backend/requirements.txt:43`
- **Evidence**:
  `fpdf2` is distributed under the GNU Lesser General Public License v3.0 (LGPLv3).
- **Impact**: If PRISM is packaged or distributed as a proprietary commercial binary or closed-source desktop application (e.g. via PyInstaller or Electron), LGPLv3 requires that end users be able to replace or relink the LGPL component, or access source code under LGPL terms.
- **Root cause**: Use of LGPL library in reporting generator without review of distribution licensing.
- **Recommended fix**: Ensure backend remains dynamically linked as Python source, document LGPL notice in third-party licenses, or replace `fpdf2` with an Apache/MIT alternative (e.g., `Weasyprint` or `ReportLab` open-source).
- **Confidence**: Medium
- **Label**: [VERIFIED]
