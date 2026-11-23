"""Patch engine.py and ml_readiness.py and story.py and target_detector.py in one shot."""
import re

# ─────────────────────────────────────────────────────────────────────────────
# 1. profiling/engine.py — persist target state after cross-column assembly
# ─────────────────────────────────────────────────────────────────────────────
path = r'c:\abbu\backend\profiling\engine.py'
with open(path, 'r', encoding='utf-8') as f:
    src = f.read().replace('\r\n', '\n').replace('\r', '\n')

marker = "    ctx.dataset.cross_analysis = cross_profile.model_dump()\n\n\ndef _stage_insights"
insert = (
    "    ctx.dataset.cross_analysis = cross_profile.model_dump()\n\n"
    "    # ── Persist the authoritative target state ──────────────────────────\n"
    "    # This is the ONLY write to target_store in the entire codebase.\n"
    "    # All downstream modules (ml_readiness, story, feature_ranker) must\n"
    "    # read via get_target_state() -- never re-run TargetDetector.analyze().\n"
    "    try:\n"
    "        from state import set_target_state\n"
    "        set_target_state(\n"
    "            ctx.file_id,\n"
    "            ctx.target_analysis or TargetAnalysis(is_target_detected=False),\n"
    "        )\n"
    "    except Exception as _e:\n"
    "        ctx.warnings.append(f'Target state persistence failed (non-fatal): {_e}')\n\n\n"
    "def _stage_insights"
)

if marker not in src:
    raise RuntimeError("engine.py marker not found")
result = src.replace(marker, insert, 1)
with open(path, 'w', encoding='utf-8', newline='\r\n') as f:
    f.write(result)
print("OK: engine.py patched")
assert 'set_target_state' in open(path, encoding='utf-8').read(), "FAIL: engine.py"

# ─────────────────────────────────────────────────────────────────────────────
# 2. profiling/target_detector.py — add temporal metadata to ANTI_HINTS
# ─────────────────────────────────────────────────────────────────────────────
path = r'c:\abbu\backend\profiling\target_detector.py'
with open(path, 'r', encoding='utf-8') as f:
    src = f.read().replace('\r\n', '\n').replace('\r', '\n')

old_anti = (
    "    ANTI_HINTS = [\n"
    "        'id', 'index', 'idx', 'key', 'pk',\n"
    "        'name', 'first_name', 'last_name', 'username',\n"
    "        'date', 'time', 'timestamp', 'created', 'updated', 'modified',\n"
    "        'url', 'uri', 'link', 'path',\n"
    "        'email', 'phone', 'address', 'city', 'state', 'zip', 'country',\n"
    "        'description', 'comment', 'note', 'text', 'body',\n"
    "        'file', 'image', 'photo', 'avatar',\n"
    "    ]"
)
new_anti = (
    "    ANTI_HINTS = [\n"
    "        'id', 'index', 'idx', 'key', 'pk',\n"
    "        'name', 'first_name', 'last_name', 'username',\n"
    "        'date', 'time', 'timestamp', 'created', 'updated', 'modified',\n"
    "        'url', 'uri', 'link', 'path',\n"
    "        'email', 'phone', 'address', 'city', 'state', 'zip', 'country',\n"
    "        'description', 'comment', 'note', 'text', 'body',\n"
    "        'file', 'image', 'photo', 'avatar',\n"
    "        # Temporal metadata columns — never prediction targets\n"
    "        'year', 'month', 'quarter', 'week', 'weekday', 'hour', 'day',\n"
    "        'hour_bucket', 'month_name',\n"
    "    ]"
)
if old_anti not in src:
    raise RuntimeError("target_detector.py ANTI_HINTS marker not found")
result = src.replace(old_anti, new_anti, 1)
with open(path, 'w', encoding='utf-8', newline='\r\n') as f:
    f.write(result)
print("OK: target_detector.py ANTI_HINTS patched")

# ─────────────────────────────────────────────────────────────────────────────
# 3. api/ml_readiness.py — read shared target state; fix DatasetProfile arg bug
# ─────────────────────────────────────────────────────────────────────────────
path = r'c:\abbu\backend\api\ml_readiness.py'
with open(path, 'r', encoding='utf-8') as f:
    src = f.read().replace('\r\n', '\n').replace('\r', '\n')

old_ml = (
    "    cross_analysis = getattr(profile, \"cross_column_analysis\", None)\n"
    "    if not cross_analysis:\n"
    "        raise HTTPException(status_code=400, detail=\"Cross-column profiling data is missing\")\n"
    "\n"
    "    # 1. Target Detection\n"
    "    detector = TargetDetector()\n"
    "    target_analysis = detector.analyze(df, profile, cross_analysis)\n"
    "\n"
    "    if not target_analysis.is_target_detected:\n"
    "        return {\n"
    "            \"status\": \"not_ready\",\n"
    "            \"score\": 0,\n"
    "            \"message\": \"No suitable target variable detected. Cannot assess ML readiness without a target.\",\n"
    "            \"target\": None,\n"
    "        }\n"
    "\n"
    "    target = target_analysis.target_column\n"
    "    problem_type = target_analysis.problem_type"
)
new_ml = (
    "    # 1. Target Detection — read from the authoritative shared state.\n"
    "    # DO NOT re-run TargetDetector.analyze() here; that would produce an\n"
    "    # independent result that can diverge from what profiling detected.\n"
    "    from state import get_target_state\n"
    "    target_state = get_target_state(file_id)\n"
    "\n"
    "    if target_state is None:\n"
    "        # Profiling hasn't run yet, or the persistence step failed.\n"
    "        # Fall back to running TargetDetector once and persisting the result.\n"
    "        from profiling.target_detector import TargetDetector as _TD\n"
    "        from state import set_target_state\n"
    "        stored_result = get_profile(file_id)\n"
    "        # get_profile returns the ProfilingResult object; DatasetProfile is .profile\n"
    "        dataset_profile = (\n"
    "            getattr(stored_result, 'profile', None)\n"
    "            or (stored_result.get('profile') if isinstance(stored_result, dict) else None)\n"
    "            or profile  # last resort: profile was already fetched above\n"
    "        )\n"
    "        cross_analysis_raw = getattr(dataset_profile, 'cross_analysis', None) or {}\n"
    "        from profiling.cross_column_models import CorrelationAnalysis\n"
    "        from profiling.correlation_analyzer import CorrelationAnalyzer\n"
    "        corr_obj = CorrelationAnalysis(\n"
    "            correlation_matrix=cross_analysis_raw.get('correlations', {}).get('correlation_matrix', {})\n"
    "            if isinstance(cross_analysis_raw, dict) else {},\n"
    "            strongest_pairs=[],\n"
    "            mutual_information={},\n"
    "        )\n"
    "        _detector = _TD()\n"
    "        _ta = _detector.analyze(df, dataset_profile, corr_obj)\n"
    "        set_target_state(file_id, _ta)\n"
    "        target_state = get_target_state(file_id)\n"
    "\n"
    "    if not target_state or not target_state.get('target_column'):\n"
    "        return {\n"
    "            'status': 'not_ready',\n"
    "            'score': 0,\n"
    "            'message': 'No suitable target variable detected. Profile the dataset first.',\n"
    "            'target': None,\n"
    "            'is_heuristic': True,\n"
    "        }\n"
    "\n"
    "    target = target_state['target_column']\n"
    "    problem_type = target_state.get('problem_type')\n"
    "    is_heuristic = target_state.get('is_heuristic', True)\n"
    "    confidence = target_state.get('confidence', 0.0)"
)
if old_ml not in src:
    raise RuntimeError("ml_readiness.py target block not found")
result = src.replace(old_ml, new_ml, 1)

# Also remove the now-unused imports
result = result.replace(
    "    from profiling.target_detector import TargetDetector\n"
    "    from cleaning.imbalance_handler import ImbalanceHandler\n",
    "    from cleaning.imbalance_handler import ImbalanceHandler\n",
    1,
)

# Patch the return dict to include is_heuristic and confidence
old_return = (
    "    return {\n"
    "        \"status\": \"ready\" if score >= 60 else \"needs_cleaning\",\n"
    "        \"score\": score,\n"
    "        \"grade\": grade,\n"
    "        \"problem_type\": problem_type,\n"
    "        \"target_column\": target,\n"
    "        \"estimated_performance_ceiling\": ceiling,"
)
new_return = (
    "    candidate_note = (\n"
    "        f\"No target was explicitly specified. '{target}' is a candidate target based on heuristic analysis.\"\n"
    "        if is_heuristic else None\n"
    "    )\n\n"
    "    return {\n"
    "        \"status\": \"ready\" if score >= 60 else \"needs_cleaning\",\n"
    "        \"score\": score,\n"
    "        \"grade\": grade,\n"
    "        \"problem_type\": problem_type,\n"
    "        \"target_column\": target,\n"
    "        \"is_user_defined\": not is_heuristic,\n"
    "        \"is_heuristic\": is_heuristic,\n"
    "        \"confidence\": confidence,\n"
    "        \"candidate_note\": candidate_note,\n"
    "        \"estimated_performance_ceiling\": ceiling,"
)
result = result.replace(old_return, new_return, 1)

with open(path, 'w', encoding='utf-8', newline='\r\n') as f:
    f.write(result)
print("OK: ml_readiness.py patched")
content_check = open(path, encoding='utf-8').read()
assert 'get_target_state' in content_check, "FAIL: ml_readiness.py missing get_target_state"
assert 'is_heuristic' in content_check, "FAIL: ml_readiness.py missing is_heuristic"

# ─────────────────────────────────────────────────────────────────────────────
# 4. api/story.py — fix slide 8 target inference + add /target override endpoint
# ─────────────────────────────────────────────────────────────────────────────
path = r'c:\abbu\backend\api\story.py'
with open(path, 'r', encoding='utf-8') as f:
    src = f.read().replace('\r\n', '\n').replace('\r', '\n')

# 4a. Fix slide 8 (lines 300-328) — replace ad-hoc target_col inference
old_slide8 = (
    "        # \u2500\u2500 8. Predictive Drivers & Target Analysis \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
    "        target_bullets = []\n"
    "        target_col = \"value\" if \"value\" in df.columns else (numeric_cols[0] if numeric_cols else df.columns[0])\n"
    "        \n"
    "        if insights:\n"
    "            rankings = getattr(insights, \"feature_importance\", None) or (insights.get(\"feature_ranking\", None) if isinstance(insights, dict) else None)\n"
    "            if rankings and isinstance(rankings, list):\n"
    "                for r in rankings[:4]:\n"
    "                    name = getattr(r, \"feature\", \"\") or (r.get(\"feature\", \"\") if isinstance(r, dict) else \"\")\n"
    "                    score = getattr(r, \"importance_score\", 0) or (r.get(\"importance_score\", 0) if isinstance(r, dict) else 0)\n"
    "                    target_bullets.append(f\"Predictive Driver '{name}': Importance Score = {score:.1f}/100.\")\n"
    "\n"
    "        if not target_bullets:\n"
    "            target_bullets = [\n"
    "                f\"Primary target metric candidate identified as '{target_col}'.\",\n"
    "                \"Secondary predictive drivers include business size tier, industry classification, and location attributes.\"\n"
    "            ]\n"
    "\n"
    "        slides.append({\n"
    "            \"id\": _uid(),\n"
    "            \"type\": \"insight\",\n"
    "            \"category\": \"Predictive Intelligence\",\n"
    "            \"title\": \"Key Predictive Drivers\",\n"
    "            \"subtitle\": f\"Target metric '{target_col}' feature attribution ranking\",\n"
    "            \"content\": \"\\n\".join(f\"\\u2022 {b}\" for b in target_bullets),\n"
    "            \"bullets\": target_bullets,\n"
    "            \"badge\": \"TARGET ANALYSIS\",\n"
    "            \"tags\": [\"Machine Learning\", \"Feature Importance\"]\n"
    "        })"
)
new_slide8 = (
    "        # \u2500\u2500 8. Predictive Drivers & Target Analysis \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
    "        # Read the authoritative target state produced by the profiling engine.\n"
    "        # NEVER independently infer target_col from column position or numeric list.\n"
    "        from state import get_target_state\n"
    "        _target_state = get_target_state(request.file_id) or {}\n"
    "        target_col = _target_state.get(\"target_column\")\n"
    "        _is_user_defined = _target_state.get(\"is_user_defined\", False)\n"
    "        _is_heuristic = _target_state.get(\"is_heuristic\", True)\n"
    "        _confidence = _target_state.get(\"confidence\", 0.0)\n"
    "\n"
    "        # Build slide subtitle based on target confidence\n"
    "        if target_col and _is_user_defined:\n"
    "            slide8_subtitle = f\"Target metric '{target_col}' feature attribution ranking (user-confirmed)\"\n"
    "        elif target_col and _is_heuristic:\n"
    "            slide8_subtitle = (\n"
    "                f\"No target was explicitly specified. '{target_col}' is a candidate target \"\n"
    "                f\"based on heuristic analysis (confidence: {_confidence:.0%}).\"\n"
    "            )\n"
    "        else:\n"
    "            slide8_subtitle = \"No target column identified -- feature importance relative to all columns\"\n"
    "\n"
    "        target_bullets = []\n"
    "        if insights:\n"
    "            rankings = getattr(insights, \"feature_importance\", None) or (insights.get(\"feature_ranking\", None) if isinstance(insights, dict) else None)\n"
    "            if rankings and isinstance(rankings, list):\n"
    "                for r in rankings[:4]:\n"
    "                    name = getattr(r, \"feature\", \"\") or (r.get(\"feature\", \"\") if isinstance(r, dict) else \"\")\n"
    "                    score = getattr(r, \"importance_score\", 0) or (r.get(\"importance_score\", 0) if isinstance(r, dict) else 0)\n"
    "                    target_bullets.append(f\"Predictive Driver '{name}': Importance Score = {score:.1f}/100.\")\n"
    "\n"
    "        if not target_bullets:\n"
    "            if target_col:\n"
    "                target_bullets = [\n"
    "                    f\"Primary target metric candidate identified as '{target_col}'.\",\n"
    "                    \"Secondary predictive drivers include business size tier, industry classification, and location attributes.\"\n"
    "                ]\n"
    "            else:\n"
    "                target_bullets = [\n"
    "                    \"No target column was identified. Upload a labeled dataset to enable predictive analysis.\",\n"
    "                    \"Feature importance is shown relative to all columns without a defined target.\"\n"
    "                ]\n"
    "\n"
    "        slides.append({\n"
    "            \"id\": _uid(),\n"
    "            \"type\": \"insight\",\n"
    "            \"category\": \"Predictive Intelligence\",\n"
    "            \"title\": \"Key Predictive Drivers\",\n"
    "            \"subtitle\": slide8_subtitle,\n"
    "            \"content\": \"\\n\".join(f\"\\u2022 {b}\" for b in target_bullets),\n"
    "            \"bullets\": target_bullets,\n"
    "            \"badge\": \"TARGET ANALYSIS\",\n"
    "            \"tags\": [\"Machine Learning\", \"Feature Importance\"],\n"
    "            \"target_column\": target_col,\n"
    "            \"is_heuristic\": _is_heuristic,\n"
    "            \"is_user_defined\": _is_user_defined,\n"
    "        })"
)
if old_slide8 not in src:
    # Try to find what's there
    idx = src.find("# \u2500\u2500 8. Predictive Drivers")
    print(f"Slide 8 marker found at idx {idx}")
    print(repr(src[idx:idx+600]))
    raise RuntimeError("story.py slide 8 block not found")

result = src.replace(old_slide8, new_slide8, 1)

# 4b. Add POST /api/story/target endpoint at the end of the router section
# Insert before the first @router.post("/export") line
old_export = '@router.post("/export")\nasync def export_story'
new_target_endpoint = (
    '@router.post("/target")\n'
    'async def set_story_target(request: dict):\n'
    '    """\n'
    '    Override the heuristic target column with an explicit user selection.\n'
    '\n'
    '    Body: {"file_id": str, "target_column": str}\n'
    '\n'
    '    Once called, all downstream modules (ml_readiness, story slide 8,\n'
    '    feature_ranker) will use this user-confirmed target instead of the\n'
    '    heuristic candidate.\n'
    '    """\n'
    '    from state import get_df, set_user_target\n'
    '    file_id = request.get("file_id")\n'
    '    target_column = request.get("target_column")\n'
    '    if not file_id or not target_column:\n'
    '        raise HTTPException(status_code=422, detail="Both file_id and target_column are required.")\n'
    '    df = get_df(file_id)\n'
    '    if df is None:\n'
    '        raise HTTPException(status_code=404, detail="Dataset not found.")\n'
    '    try:\n'
    '        state = set_user_target(file_id, target_column, df.columns.tolist())\n'
    '    except ValueError as e:\n'
    '        raise HTTPException(status_code=400, detail=str(e))\n'
    '    return {\n'
    '        "status": "ok",\n'
    '        "message": f"Target column set to \'{target_column}\'. All downstream analyses will use this target.",\n'
    '        "target_state": state,\n'
    '    }\n'
    '\n'
    '\n'
    '@router.post("/export")\n'
    'async def export_story'
)
if old_export not in result:
    raise RuntimeError("story.py /export marker not found")
result = result.replace(old_export, new_target_endpoint, 1)

with open(path, 'w', encoding='utf-8', newline='\r\n') as f:
    f.write(result)
print("OK: story.py patched (slide 8 + /target endpoint)")
content_check = open(path, encoding='utf-8').read()
assert 'get_target_state' in content_check, "FAIL: story.py missing get_target_state"
assert 'set_user_target' in content_check, "FAIL: story.py missing set_user_target"
assert 'is_heuristic' in content_check, "FAIL: story.py missing is_heuristic"
assert 'slide8_subtitle' in content_check, "FAIL: story.py missing slide8_subtitle"

print("\nAll patches applied successfully.")
