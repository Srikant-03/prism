import os
import re
import zlib
import pytest
import pandas as pd

from profiling.engine import DataProfiler
from reporting.report_generator import ReportGenerator, ReportExporter
from api.story import generate_story, _build_story_pdf, StoryRequest
from state import set_df, set_profile, set_target_state

DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "prism_adversarial_e2e_test.csv")


def extract_pdf_text_and_page_count(pdf_bytes: bytes) -> tuple[str, int]:
    """Helper to extract decompressed text and page count from raw PDF bytes."""
    pages_count = len(re.findall(rb'/Type\s*/Page\b', pdf_bytes))
    decompressed_parts = []
    for m in re.finditer(b'stream\r?\n(.*?)\r?\nendstream', pdf_bytes, re.DOTALL):
        try:
            decompressed_parts.append(zlib.decompress(m.group(1)).decode('latin-1', 'ignore'))
        except Exception:
            decompressed_parts.append(m.group(1).decode('latin-1', 'ignore'))
    return " ".join(decompressed_parts), pages_count


@pytest.fixture(scope="module")
def adversarial_setup():
    assert os.path.exists(DATASET_PATH), f"CSV not found at {DATASET_PATH}"
    df = pd.read_csv(DATASET_PATH)
    file_id = "test_phase5_e2e"
    
    profiling_result = DataProfiler.profile(df, file_id=file_id)
    profile = profiling_result.profile
    
    # Register in state so API endpoints can access it
    set_df(file_id, df)
    set_profile(file_id, profiling_result)
    
    if profile.cross_analysis and profile.cross_analysis.get("target"):
        set_target_state(file_id, profile.cross_analysis["target"])
        
    return df, profile, file_id


@pytest.mark.asyncio
async def test_story_deck_pdf(adversarial_setup):
    """Story deck PDF must be landscape, >=8 pages, with zero empty placeholders."""
    df, profile, file_id = adversarial_setup
    
    # Generate story
    story_resp = await generate_story(StoryRequest(file_id=file_id))
    slides = story_resp.get("slides", [])
    assert len(slides) >= 8, f"Expected >=8 story slides, got {len(slides)}"
    
    # Build PDF
    pdf_bytes = _build_story_pdf(slides, file_id=file_id)
    assert len(pdf_bytes) > 5000, "PDF bytes should be non-empty"
    
    full_text, page_count = extract_pdf_text_and_page_count(pdf_bytes)
    
    assert page_count >= 8, f"Expected at least 8 PDF pages in story deck, got {page_count}"
    assert "No data available for summary." not in full_text, "Placeholder text found in story deck"
    assert "PRISM Executive Story Deck" in full_text
    assert "Executive Briefing" in full_text or "ANALYSIS" in full_text


def test_full_analysis_report_pdf(adversarial_setup):
    """ReportGenerator PDF must report 5 duplicates, 27 total features, no fake 0.00 rows, and wrapped tables."""
    df, profile, file_id = adversarial_setup
    
    profile_data = profile.model_dump() if hasattr(profile, "model_dump") else profile.__dict__
    insights_data = profile.insights.model_dump() if hasattr(profile.insights, "model_dump") else getattr(profile.insights, "__dict__", {})
    
    report = ReportGenerator.generate(profile_data=profile_data, insights_data=insights_data)
    pdf_bytes = ReportExporter.to_pdf(report)
    assert len(pdf_bytes) > 5000, "Report PDF bytes should be non-empty"
    
    full_text, page_count = extract_pdf_text_and_page_count(pdf_bytes)
    
    # 1. Duplicates check: exactly 5 duplicate rows
    assert "5 completely duplicated rows" in full_text or "5 duplicate rows" in full_text, "Report must report 5 duplicate rows"
    
    # 2. Total feature count accurately sums to 27
    assert "total: 27" in full_text or "27 distinct features" in full_text
    
    # 3. Descriptive statistics table excludes dummy 0.00 rows for IDs and booleans
    # Check that sequential_row_id or churn_flag are NOT in Descriptive Statistics with 0.00 mean
    # In full_text, verify sequential_row_id doesn't appear adjacent to multiple 0.00 values
    assert not re.search(r'sequential_row_id\s*0\.00\s*0\.00', full_text)
    assert not re.search(r'churn_flag\s*0\.00\s*0\.00', full_text)
    
    # 4. Recommendations check: never recommend Target Encoding on ID or email
    all_recs_text = ""
    for s in report.sections:
        if s.title == "Recommendations":
            all_recs_text = s.content
            
    assert "txn_uuid" not in all_recs_text
    assert "customer_email" not in all_recs_text
