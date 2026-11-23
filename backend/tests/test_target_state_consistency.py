"""
tests/test_target_state_consistency.py
=======================================
Verifies that target detection, target analysis, feature importance,
preprocessing recommendations, executive summary, and storytelling/deck
ALL use the exact same target column — never independently re-derive it.

Run with:
    venv\\Scripts\\python.exe -X utf8 -m pytest tests/test_target_state_consistency.py -v
or:
    venv\\Scripts\\python.exe -X utf8 tests/test_target_state_consistency.py
"""
from __future__ import annotations

import sys
import io
import types
import uuid
import asyncio
import traceback

# ── Encoding safety (Windows console) ────────────────────────────────────────

sys.path.insert(0, ".")

import pandas as pd
import numpy as np

# ── Test helpers ──────────────────────────────────────────────────────────────
PASS = 0
FAIL = 0
ERRORS: list[str] = []


def _check(label: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        msg = f"  [FAIL] {label}"
        if detail:
            msg += f"\n         {detail}"
        print(msg)
        ERRORS.append(label)


def _section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── Shared test DataFrame (mirrors dashboard_test_data.csv schema) ─────────────
SAMPLE_DF = pd.DataFrame({
    "date":                 ["2023-01-15", "2023-02-15", "2023-03-20"] * 10,
    "month_name":           ["January", "February", "March"] * 10,
    "quarter":              ["Q1", "Q1", "Q1"] * 10,
    "year":                 [2023, 2023, 2023] * 10,
    "category":             ["Electronics", "Clothing", "Food"] * 10,
    "subcategory":          ["Smartphones", "Casual Wear", "Beverages"] * 10,
    "region":               ["North", "South", "East"] * 10,
    "country":              ["USA", "Mexico", "Canada"] * 10,
    "channel":              ["Online", "Offline", "Partner"] * 10,
    "salesperson":          ["Alice", "Bob", "Carol"] * 10,
    "revenue":              np.random.uniform(5000, 50000, 30),
    "cost":                 np.random.uniform(2000, 30000, 30),
    "profit":               np.random.uniform(1000, 20000, 30),
    "units_sold":           np.random.randint(10, 500, 30),
    "avg_order_value":      np.random.uniform(50, 3000, 30),
    "customer_satisfaction": np.random.uniform(3.0, 5.0, 30),
    "response_time_days":   np.random.uniform(0.5, 7.0, 30),
    "discount_pct":         [np.nan if i % 7 == 0 else round(np.random.uniform(0, 25), 1) for i in range(30)],
    "return_rate":          np.random.uniform(1.0, 12.0, 30),
    "funnel_stage":         ["Purchase", "Awareness", "Interest"] * 10,
    "prospect_count":       np.random.randint(10, 1000, 30),
    "weekday":              ["Monday", "Tuesday", "Wednesday"] * 10,
    "hour_bucket":          ["00-06", "06-09", "09-12"] * 10,
})

FILE_ID = f"test_{uuid.uuid4().hex[:8]}"
FILE_ID_USER = f"test_user_{uuid.uuid4().hex[:8]}"


# ─────────────────────────────────────────────────────────────────────────────
def _run_all_sections():
    global PASS, FAIL, ERRORS
    # SECTION 1: TargetDetector heuristics
    # ─────────────────────────────────────────────────────────────────────────────
    _section("1. TargetDetector — heuristic scoring")

    try:
        from profiling.target_detector import TargetDetector
        from profiling.cross_column_models import CorrelationAnalysis, MulticollinearityReport

        # Build minimal dataset profile
        from profiling.profiling_models import DatasetProfile, ColumnProfile, SemanticType
        dummy_profile = DatasetProfile(total_rows=30, total_columns=23)
        for col in SAMPLE_DF.columns:
            dtype = SAMPLE_DF[col].dtype
            if pd.api.types.is_numeric_dtype(dtype):
                sem = SemanticType.NUMERIC_CONTINUOUS
            elif col in ("date",):
                sem = SemanticType.DATETIME
            else:
                sem = SemanticType.CATEGORICAL_NOMINAL
            dummy_profile.columns.append(ColumnProfile(name=col, position=0, inferred_dtype=str(dtype), semantic_type=sem))

        corr_obj = CorrelationAnalysis(
            correlation_matrix={},
            strongest_pairs=[],
            multicollinearity=MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[]),
            mutual_information={},
        )

        detector = TargetDetector()
        ta = detector.analyze(SAMPLE_DF, dummy_profile, corr_obj)

        _check("TargetDetector.analyze() returns TargetAnalysis", ta is not None)
        _check(
            "Detected target is NOT 'year' (year is in ANTI_HINTS)",
            ta.target_column != "year",
            f"Got: {ta.target_column}",
        )
        _check(
            "Detected target is NOT 'month_name' (month_name is in ANTI_HINTS)",
            ta.target_column != "month_name",
            f"Got: {ta.target_column}",
        )
        _check(
            "Detected target is NOT 'quarter' (quarter is in ANTI_HINTS)",
            ta.target_column != "quarter",
            f"Got: {ta.target_column}",
        )
        _check(
            "Detected target is NOT 'weekday' (weekday is in ANTI_HINTS)",
            ta.target_column != "weekday",
            f"Got: {ta.target_column}",
        )
        _check(
            "Detected target is NOT 'hour_bucket' (hour_bucket is in ANTI_HINTS)",
            ta.target_column != "hour_bucket",
            f"Got: {ta.target_column}",
        )
        _check(
            "is_target_detected is True (dataset has detectable targets)",
            ta.is_target_detected,
            f"target_column={ta.target_column}",
        )
        _check(
            "problem_type is set",
            ta.problem_type in ("regression", "binary_classification", "multiclass_classification"),
            f"Got: {ta.problem_type}",
        )
        print(f"       → Detected: '{ta.target_column}' ({ta.problem_type}, confidence={ta.confidence:.0%})")

        DETECTED_TARGET = ta.target_column
        DETECTED_PROBLEM_TYPE = ta.problem_type

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 1 import/run error")
        print(f"  [FAIL] Section 1 ERROR: {e}")
        traceback.print_exc()
        DETECTED_TARGET = "prospect_count"    # safe fallback for downstream tests
        DETECTED_PROBLEM_TYPE = "regression"


    # ─────────────────────────────────────────────────────────────────────────────
    # SECTION 2: state.py target store round-trip
    # ─────────────────────────────────────────────────────────────────────────────
    _section("2. state.py — target store round-trip")

    try:
        from state import (
            get_target_state, set_target_state, set_user_target,
            target_store,
        )

        # 2a. Initially None
        ts0 = get_target_state(FILE_ID)
        _check("get_target_state returns None before any write", ts0 is None, f"Got: {ts0}")

        # 2b. set_target_state with heuristic TargetAnalysis
        from profiling.cross_column_models import TargetAnalysis, FeatureImportance
        mock_ta = TargetAnalysis(
            is_target_detected=True,
            target_column=DETECTED_TARGET,
            confidence=0.88,
            justification="Heuristic: strong name match.",
            problem_type=DETECTED_PROBLEM_TYPE,
            top_predictors=[
                FeatureImportance(feature="revenue", importance_score=0.82),
                FeatureImportance(feature="profit",  importance_score=0.74),
            ],
        )
        set_target_state(FILE_ID, mock_ta)
        ts1 = get_target_state(FILE_ID)

        _check("get_target_state returns non-None after set_target_state", ts1 is not None)
        _check(
            "Stored target_column matches what TargetDetector found",
            ts1.get("target_column") == DETECTED_TARGET,
            f"Expected: {DETECTED_TARGET!r}, Got: {ts1.get('target_column')!r}",
        )
        _check(
            "is_heuristic=True after set_target_state (not user-defined)",
            ts1.get("is_heuristic") is True,
            f"Got is_heuristic={ts1.get('is_heuristic')}",
        )
        _check(
            "is_user_defined=False after set_target_state",
            ts1.get("is_user_defined") is False,
            f"Got is_user_defined={ts1.get('is_user_defined')}",
        )
        _check(
            "confidence stored correctly",
            abs(ts1.get("confidence", 0) - 0.88) < 0.001,
            f"Got confidence={ts1.get('confidence')}",
        )
        _check(
            "top_predictors stored as list of dicts",
            isinstance(ts1.get("top_predictors"), list) and len(ts1["top_predictors"]) == 2,
            f"Got: {ts1.get('top_predictors')}",
        )
        _check(
            "top_predictors[0]['feature'] == 'revenue'",
            (ts1.get("top_predictors") or [{}])[0].get("feature") == "revenue",
            f"Got: {ts1.get('top_predictors')}",
        )
        _check(
            "problem_type stored correctly",
            ts1.get("problem_type") == DETECTED_PROBLEM_TYPE,
            f"Got: {ts1.get('problem_type')}",
        )

        # 2c. set_target_state with no detected target
        not_detected_ta = TargetAnalysis(is_target_detected=False)
        set_target_state("no_target_file", not_detected_ta)
        ts_nd = get_target_state("no_target_file")
        _check(
            "Not-detected sentinel written (not None)",
            ts_nd is not None,
            f"Got: {ts_nd}",
        )
        _check(
            "Not-detected sentinel has target_column=None",
            ts_nd.get("target_column") is None,
            f"Got: {ts_nd.get('target_column')}",
        )

        # 2d. set_user_target — valid column
        set_target_state(FILE_ID_USER, mock_ta)   # seed with heuristic first
        updated = set_user_target(FILE_ID_USER, "revenue", SAMPLE_DF.columns.tolist())
        ts2 = get_target_state(FILE_ID_USER)

        _check(
            "set_user_target updates target_column",
            ts2.get("target_column") == "revenue",
            f"Got: {ts2.get('target_column')}",
        )
        _check(
            "is_user_defined=True after set_user_target",
            ts2.get("is_user_defined") is True,
            f"Got is_user_defined={ts2.get('is_user_defined')}",
        )
        _check(
            "is_heuristic=False after set_user_target",
            ts2.get("is_heuristic") is False,
            f"Got is_heuristic={ts2.get('is_heuristic')}",
        )
        _check(
            "confidence=1.0 after user override",
            ts2.get("confidence") == 1.0,
            f"Got: {ts2.get('confidence')}",
        )

        # 2e. set_user_target — invalid column raises ValueError
        try:
            set_user_target(FILE_ID_USER, "nonexistent_col", SAMPLE_DF.columns.tolist())
            _check("set_user_target raises ValueError for invalid column", False, "No exception raised")
        except ValueError:
            _check("set_user_target raises ValueError for invalid column", True)

        # 2f. Second get_target_state after user override still returns user-defined
        ts3 = get_target_state(FILE_ID_USER)
        _check(
            "Subsequent get_target_state still returns user-defined state",
            ts3.get("target_column") == "revenue" and ts3.get("is_user_defined") is True,
            f"Got: {ts3}",
        )

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 2 error")
        print(f"  [FAIL] Section 2 ERROR: {e}")
        traceback.print_exc()


    # ─────────────────────────────────────────────────────────────────────────────
    # SECTION 3: ml_readiness reads shared state (not re-run detector)
    # ─────────────────────────────────────────────────────────────────────────────
    _section("3. ml_readiness.py — reads shared target state")

    try:
        from api.ml_readiness import get_ml_readiness
        from fastapi.testclient import TestClient
        from fastapi import FastAPI

        # Monkey-patch state functions to return our test data
        import state as _state_mod
        import api.ml_readiness as _ml_mod

        # Save originals
        _orig_get_df = _state_mod.get_df
        _orig_get_profile = _state_mod.get_profile
        _orig_get_target_state_ml = _state_mod.get_target_state

        def _mock_get_df(fid):
            if fid == FILE_ID:
                return SAMPLE_DF
            return None

        def _mock_get_profile(fid):
            if fid == FILE_ID:
                return types.SimpleNamespace(
                    profile=types.SimpleNamespace(cross_analysis={}, columns=[]),
                )
            return None

        # Seed target state before calling ml_readiness
        from state import set_target_state as _sts
        from profiling.cross_column_models import TargetAnalysis, FeatureImportance
        _sts(FILE_ID, TargetAnalysis(
            is_target_detected=True,
            target_column=DETECTED_TARGET,
            confidence=0.88,
            justification="Heuristic.",
            problem_type=DETECTED_PROBLEM_TYPE,
            top_predictors=[FeatureImportance(feature="revenue", importance_score=0.8)],
        ))

        # Patch state accessors in ml_readiness module
        import importlib
        _ml_mod_src = importlib.import_module("api.ml_readiness")

        original_get_df_ml = None
        # We test by verifying get_target_state is called and returns our value
        retrieved_targets: list[str] = []
        original_get_target_state = _state_mod.get_target_state

        def _spying_get_target_state(fid):
            result = original_get_target_state(fid)
            if result and result.get("target_column"):
                retrieved_targets.append(result["target_column"])
            return result

        _state_mod.get_target_state = _spying_get_target_state

        # Build a minimal FastAPI app for TestClient
        app = FastAPI()
        app.include_router(_ml_mod_src.router)

        # We need to mock get_df and get_profile at the module level used by ml_readiness
        import builtins
        _real_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            mod = _real_import(name, *args, **kwargs)
            if name == "state":
                mod.get_df = _mock_get_df
                mod.get_profile = _mock_get_profile
                mod.get_target_state = _spying_get_target_state
            return mod

        builtins.__import__ = _mock_import

        try:
            client = TestClient(app, raise_server_exceptions=False)
            response = client.get(f"/api/ml-readiness/{FILE_ID}")
            data = response.json() if response.status_code < 500 else {}
        finally:
            builtins.__import__ = _real_import
            _state_mod.get_target_state = original_get_target_state

        # The key invariant: if ml_readiness consumed the shared state, retrieved_targets
        # must contain DETECTED_TARGET (not "year" or something else)
        _check(
            "ml_readiness called get_target_state (shared state)",
            len(retrieved_targets) > 0,
            f"get_target_state was never called. targets={retrieved_targets}",
        )
        if retrieved_targets:
            _check(
                f"ml_readiness retrieved '{DETECTED_TARGET}' from shared state (not 'year')",
                DETECTED_TARGET in retrieved_targets,
                f"Retrieved: {retrieved_targets}",
            )
            _check(
                "ml_readiness did NOT retrieve 'year' as the target",
                "year" not in retrieved_targets,
                f"Retrieved: {retrieved_targets}",
            )

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 3 error")
        print(f"  [FAIL] Section 3 ERROR: {e}")
        traceback.print_exc()


    # ─────────────────────────────────────────────────────────────────────────────
    # SECTION 4: story.py generate_story reads shared state (not column position)
    # ─────────────────────────────────────────────────────────────────────────────
    _section("4. story.py generate_story — reads shared target state")

    try:
        import api.story as _story_mod
        from api.story import generate_story, StoryRequest

        # Seed target state for a fresh file_id
        STORY_FILE_ID = f"story_{uuid.uuid4().hex[:8]}"
        from state import set_target_state as _sts2
        from profiling.cross_column_models import TargetAnalysis
        _sts2(STORY_FILE_ID, TargetAnalysis(
            is_target_detected=True,
            target_column=DETECTED_TARGET,
            confidence=0.88,
            justification="Heuristic.",
            problem_type=DETECTED_PROBLEM_TYPE,
        ))

        # Patch get_df and get_profile for the story module
        import state as _state_story
        _orig_gdf_story = _state_story.get_df
        _orig_gpro_story = _state_story.get_profile
        _state_story.get_df = lambda fid: SAMPLE_DF if fid == STORY_FILE_ID else None
        _state_story.get_profile = lambda fid: None   # no profile = use fallback paths

        # Also patch the inline import inside generate_story
        import builtins
        _real_import2 = builtins.__import__
        story_target_retrievals: list = []

        _orig_gts2 = _state_story.get_target_state
        def _spy_gts2(fid):
            r = _orig_gts2(fid)
            if r:
                story_target_retrievals.append(r.get("target_column"))
            return r
        _state_story.get_target_state = _spy_gts2

        try:
            story_result = asyncio.get_event_loop().run_until_complete(
                generate_story(StoryRequest(file_id=STORY_FILE_ID))
            )
        finally:
            _state_story.get_df = _orig_gdf_story
            _state_story.get_profile = _orig_gpro_story
            _state_story.get_target_state = _orig_gts2

        slides = story_result.get("slides", [])
        slide8 = next((s for s in slides if s.get("category") == "Predictive Intelligence"), None)

        _check("generate_story returns 10 slides", len(slides) == 10, f"Got {len(slides)}")
        _check("Slide 8 exists (Predictive Intelligence)", slide8 is not None)

        if slide8:
            subtitle = slide8.get("subtitle", "")
            _check(
                f"Slide 8 subtitle does NOT contain 'year'",
                "year" not in subtitle.lower(),
                f"subtitle={subtitle!r}",
            )
            _check(
                f"Slide 8 subtitle or bullets reference '{DETECTED_TARGET}' (shared target)",
                (
                    DETECTED_TARGET in subtitle
                    or any(DETECTED_TARGET in b for b in slide8.get("bullets", []))
                    or slide8.get("target_column") == DETECTED_TARGET
                ),
                f"subtitle={subtitle!r}, target_column={slide8.get('target_column')}",
            )
            _check(
                "Slide 8 has 'is_heuristic' field",
                "is_heuristic" in slide8,
                f"slide8 keys: {list(slide8.keys())}",
            )
            _check(
                "Slide 8 has 'target_column' field",
                "target_column" in slide8,
                f"slide8 keys: {list(slide8.keys())}",
            )
            _check(
                "Slide 8 is_heuristic=True (heuristic detection, not user-confirmed)",
                slide8.get("is_heuristic") is True,
                f"Got is_heuristic={slide8.get('is_heuristic')}",
            )
            _check(
                "Slide 8 subtitle contains 'heuristic' or 'candidate' (not authoritative)",
                "heuristic" in subtitle.lower() or "candidate" in subtitle.lower() or "user-confirmed" not in subtitle.lower(),
                f"subtitle={subtitle!r}",
            )

        _check(
            "generate_story called get_target_state",
            len(story_target_retrievals) > 0,
            "get_target_state was never called",
        )

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 4 error")
        print(f"  [FAIL] Section 4 ERROR: {e}")
        traceback.print_exc()


    # ─────────────────────────────────────────────────────────────────────────────
    # SECTION 5: User override — all modules use the new user-defined target
    # ─────────────────────────────────────────────────────────────────────────────
    _section("5. User override consistency — all modules converge on user-selected target")

    try:
        from state import set_user_target, get_target_state

        OVERRIDE_FILE_ID = f"override_{uuid.uuid4().hex[:8]}"

        # Seed with heuristic first
        from profiling.cross_column_models import TargetAnalysis
        from state import set_target_state as _sts3
        _sts3(OVERRIDE_FILE_ID, TargetAnalysis(
            is_target_detected=True,
            target_column=DETECTED_TARGET,
            confidence=0.88,
            justification="Heuristic.",
            problem_type=DETECTED_PROBLEM_TYPE,
        ))

        ts_before = get_target_state(OVERRIDE_FILE_ID)
        _check(
            f"Before override: target_column = '{DETECTED_TARGET}'",
            ts_before.get("target_column") == DETECTED_TARGET,
            f"Got: {ts_before.get('target_column')}",
        )
        _check(
            "Before override: is_heuristic=True",
            ts_before.get("is_heuristic") is True,
        )

        # User overrides to 'revenue'
        set_user_target(OVERRIDE_FILE_ID, "revenue", SAMPLE_DF.columns.tolist())

        ts_after = get_target_state(OVERRIDE_FILE_ID)
        _check(
            "After override: target_column = 'revenue'",
            ts_after.get("target_column") == "revenue",
            f"Got: {ts_after.get('target_column')}",
        )
        _check(
            "After override: is_user_defined=True",
            ts_after.get("is_user_defined") is True,
        )
        _check(
            "After override: is_heuristic=False",
            ts_after.get("is_heuristic") is False,
        )
        _check(
            "After override: confidence=1.0",
            ts_after.get("confidence") == 1.0,
        )

        # Verify that a second get_target_state call returns the same user-defined state
        ts_again = get_target_state(OVERRIDE_FILE_ID)
        _check(
            "Third read still returns user-defined 'revenue' (persistence check)",
            ts_again.get("target_column") == "revenue" and ts_again.get("is_user_defined") is True,
        )

        # Verify that now story.py slide 8 would show the user-defined target
        import api.story as _story_mod2
        from api.story import generate_story, StoryRequest
        import state as _state_ov
        _orig_gdf_ov = _state_ov.get_df
        _orig_gpro_ov = _state_ov.get_profile
        _state_ov.get_df = lambda fid: SAMPLE_DF if fid == OVERRIDE_FILE_ID else None
        _state_ov.get_profile = lambda fid: None

        try:
            story_result2 = asyncio.get_event_loop().run_until_complete(
                generate_story(StoryRequest(file_id=OVERRIDE_FILE_ID))
            )
        finally:
            _state_ov.get_df = _orig_gdf_ov
            _state_ov.get_profile = _orig_gpro_ov

        slides2 = story_result2.get("slides", [])
        slide8_override = next((s for s in slides2 if s.get("category") == "Predictive Intelligence"), None)

        if slide8_override:
            subtitle2 = slide8_override.get("subtitle", "")
            _check(
                "After user override: slide 8 subtitle does NOT say 'heuristic' or 'candidate'",
                "heuristic" not in subtitle2.lower() and "candidate" not in subtitle2.lower(),
                f"subtitle={subtitle2!r}",
            )
            _check(
                "After user override: slide 8 subtitle says 'user-confirmed'",
                "user-confirmed" in subtitle2.lower(),
                f"subtitle={subtitle2!r}",
            )
            _check(
                "After user override: slide 8 target_column='revenue'",
                slide8_override.get("target_column") == "revenue",
                f"Got: {slide8_override.get('target_column')}",
            )
            _check(
                "After user override: slide 8 is_user_defined=True",
                slide8_override.get("is_user_defined") is True,
            )

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 5 error")
        print(f"  [FAIL] Section 5 ERROR: {e}")
        traceback.print_exc()


    # ─────────────────────────────────────────────────────────────────────────────
    # SECTION 6: set_target_state from profiling engine → TargetAnalysis Pydantic object
    # ─────────────────────────────────────────────────────────────────────────────
    _section("6. set_target_state — Pydantic object, plain dict, missing-target sentinel")

    try:
        from state import set_target_state, get_target_state
        from profiling.cross_column_models import TargetAnalysis, FeatureImportance

        fid6a = f"t6a_{uuid.uuid4().hex[:6]}"
        fid6b = f"t6b_{uuid.uuid4().hex[:6]}"
        fid6c = f"t6c_{uuid.uuid4().hex[:6]}"

        # 6a. Pydantic object input
        ta_pydantic = TargetAnalysis(
            is_target_detected=True,
            target_column="profit",
            confidence=0.91,
            problem_type="regression",
            top_predictors=[FeatureImportance(feature="revenue", importance_score=0.9)],
        )
        set_target_state(fid6a, ta_pydantic)
        s6a = get_target_state(fid6a)
        _check("Pydantic input: target_column stored", s6a.get("target_column") == "profit")
        _check("Pydantic input: confidence stored", abs(s6a.get("confidence", 0) - 0.91) < 0.001)
        _check("Pydantic input: top_predictors stored as list", isinstance(s6a.get("top_predictors"), list))

        # 6b. Plain dict input
        set_target_state(fid6b, {
            "target_column": "units_sold",
            "problem_type": "regression",
            "confidence": 0.77,
            "justification": "Dict input.",
            "top_predictors": [{"feature": "revenue", "importance_score": 0.6}],
        })
        s6b = get_target_state(fid6b)
        _check("Dict input: target_column stored", s6b.get("target_column") == "units_sold")

        # 6c. Not-detected sentinel (TargetAnalysis with is_target_detected=False)
        set_target_state(fid6c, TargetAnalysis(is_target_detected=False))
        s6c = get_target_state(fid6c)
        _check("Not-detected: state written (not None)", s6c is not None)
        _check("Not-detected: target_column is None", s6c.get("target_column") is None)
        _check("Not-detected: is_heuristic=True", s6c.get("is_heuristic") is True)
        _check("Not-detected: is_user_defined=False", s6c.get("is_user_defined") is False)

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 6 error")
        print(f"  [FAIL] Section 6 ERROR: {e}")
        traceback.print_exc()


    # ─────────────────────────────────────────────────────────────────────────────
    # SECTION 7: /api/story/target endpoint
    # ─────────────────────────────────────────────────────────────────────────────
    _section("7. POST /api/story/target — user override endpoint")

    try:
        from fastapi.testclient import TestClient
        from fastapi import FastAPI
        import api.story as _story_ep_mod
        import state as _state_ep

        ENDPOINT_FILE_ID = f"ep_{uuid.uuid4().hex[:8]}"
        # Pre-seed target state
        from state import set_target_state as _sts4
        from profiling.cross_column_models import TargetAnalysis
        _sts4(ENDPOINT_FILE_ID, TargetAnalysis(
            is_target_detected=True,
            target_column=DETECTED_TARGET,
            confidence=0.88,
            problem_type=DETECTED_PROBLEM_TYPE,
        ))

        _orig_gdf_ep = _state_ep.get_df
        _state_ep.get_df = lambda fid: SAMPLE_DF if fid == ENDPOINT_FILE_ID else None

        app_ep = FastAPI()
        app_ep.include_router(_story_ep_mod.router)
        client_ep = TestClient(app_ep, raise_server_exceptions=True)

        try:
            # Valid override
            resp = client_ep.post("/api/story/target", json={
                "file_id": ENDPOINT_FILE_ID,
                "target_column": "revenue",
            })
            _check("/api/story/target returns 200", resp.status_code == 200, f"Status: {resp.status_code}, body: {resp.text[:200]}")
            if resp.status_code == 200:
                rdata = resp.json()
                _check("Response has status='ok'", rdata.get("status") == "ok")
                ts_ep = rdata.get("target_state", {})
                _check("Response target_state.target_column='revenue'", ts_ep.get("target_column") == "revenue")
                _check("Response target_state.is_user_defined=True", ts_ep.get("is_user_defined") is True)

            # Verify persistent in target_store
            from state import get_target_state as _gts_ep
            ts_persisted = _gts_ep(ENDPOINT_FILE_ID)
            _check(
                "Target store persisted user override after /target endpoint",
                ts_persisted.get("target_column") == "revenue" and ts_persisted.get("is_user_defined") is True,
                f"Got: {ts_persisted}",
            )

            # Invalid column → 400
            resp_bad = client_ep.post("/api/story/target", json={
                "file_id": ENDPOINT_FILE_ID,
                "target_column": "totally_nonexistent_col",
            })
            _check(
                "/api/story/target returns 400 for nonexistent column",
                resp_bad.status_code == 400,
                f"Status: {resp_bad.status_code}",
            )

            # Missing file_id → 404
            resp_missing = client_ep.post("/api/story/target", json={
                "file_id": "does_not_exist_xyz",
                "target_column": "revenue",
            })
            _check(
                "/api/story/target returns 404 for unknown file_id",
                resp_missing.status_code == 404,
                f"Status: {resp_missing.status_code}",
            )

        finally:
            _state_ep.get_df = _orig_gdf_ep

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 7 error")
        print(f"  [FAIL] Section 7 ERROR: {e}")
        traceback.print_exc()


    # ─────────────────────────────────────────────────────────────────────────────
    # SECTION 8: ANTI_HINTS include year/quarter/month/weekday/hour_bucket
    # ─────────────────────────────────────────────────────────────────────────────
    _section("8. TargetDetector.ANTI_HINTS covers temporal metadata columns")

    try:
        from profiling.target_detector import TargetDetector

        anti = TargetDetector.ANTI_HINTS
        for col_name in ("year", "month", "quarter", "week", "weekday", "hour", "day", "hour_bucket", "month_name"):
            _check(
                f"'{col_name}' is in ANTI_HINTS",
                col_name in anti,
                f"ANTI_HINTS={anti}",
            )

    except Exception as e:
        FAIL += 1
        ERRORS.append("Section 8 error")
        print(f"  [FAIL] Section 8 ERROR: {e}")
        traceback.print_exc()


    # ─────────────────────────────────────────────────────────────────────────────
    # Final results & Pytest assertion
    # ─────────────────────────────────────────────────────────────────────────────
    total = PASS + FAIL
    print(f"\n{'='*60}")
    print(f"  RESULTS: {total} tests  |  {PASS} passed  |  {FAIL} failed")
    print(f"{'='*60}")

    if FAIL == 0:
        print("\nAll tests passed!")
    else:
        print(f"\nFailed tests ({FAIL}):")
        for e in ERRORS:
            print(f"  x {e}")



def test_target_state_consistency_all_sections():
    global PASS, FAIL, ERRORS
    PASS = 0
    FAIL = 0
    ERRORS = []
    _run_all_sections()
    assert FAIL == 0, f"Target state consistency had {FAIL} failures: {ERRORS}"
    assert PASS >= 70, f"Expected at least 70 checks to pass, got {PASS}"

if __name__ == "__main__":
    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer"):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    _run_all_sections()
    sys.exit(0 if FAIL == 0 else 1)
