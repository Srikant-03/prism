"""Patch story.py slide 8 and add /target endpoint."""
path = r'c:\abbu\backend\api\story.py'
with open(path, 'r', encoding='utf-8') as f:
    src = f.read().replace('\r\n', '\n').replace('\r', '\n')

# Find the exact slide 8 block boundaries
start_marker = "# \u2500\u2500 8. Predictive Drivers & Target Analysis"
end_marker = "# \u2500\u2500 9. Strategic Recommendations"

start = src.find(start_marker)
end = src.find(end_marker)

if start < 0:
    raise RuntimeError(f"Slide 8 start not found. First 50 chars around 'Predictive': {repr(src[src.find('Predictive'):src.find('Predictive')+100])}")
if end < 0:
    raise RuntimeError("Slide 8 end (slide 9 marker) not found")

# What we are replacing: the entire slide 8 block
old_block = src[start:end]
print("OLD BLOCK (first 200 chars):", repr(old_block[:200]))

new_block = (
    "# \u2500\u2500 8. Predictive Drivers & Target Analysis \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
    "        # Read the authoritative target state produced by the profiling engine.\n"
    "        # NEVER independently infer target_col from column position or numeric list.\n"
    "        from state import get_target_state\n"
    "        _target_state = get_target_state(request.file_id) or {}\n"
    "        target_col = _target_state.get('target_column')\n"
    "        _is_user_defined = _target_state.get('is_user_defined', False)\n"
    "        _is_heuristic = _target_state.get('is_heuristic', True)\n"
    "        _confidence = _target_state.get('confidence', 0.0)\n"
    "\n"
    "        # Build slide subtitle based on target confirmation status\n"
    "        if target_col and _is_user_defined:\n"
    "            slide8_subtitle = f\"Target metric '{target_col}' feature attribution ranking (user-confirmed)\"\n"
    "        elif target_col and _is_heuristic:\n"
    "            slide8_subtitle = (\n"
    "                f\"No target was explicitly specified. '{target_col}' is a candidate target \"\n"
    "                f\"based on heuristic analysis (confidence: {_confidence:.0%}).\"\n"
    "            )\n"
    "        else:\n"
    "            slide8_subtitle = 'No target column identified -- feature importance relative to all columns'\n"
    "\n"
    "        target_bullets = []\n"
    "        if insights:\n"
    "            rankings = getattr(insights, 'feature_importance', None) or (insights.get('feature_ranking', None) if isinstance(insights, dict) else None)\n"
    "            if rankings and isinstance(rankings, list):\n"
    "                for r in rankings[:4]:\n"
    "                    name = getattr(r, 'feature', '') or (r.get('feature', '') if isinstance(r, dict) else '')\n"
    "                    score = getattr(r, 'importance_score', 0) or (r.get('importance_score', 0) if isinstance(r, dict) else 0)\n"
    "                    target_bullets.append(f\"Predictive Driver '{name}': Importance Score = {score:.1f}/100.\")\n"
    "\n"
    "        if not target_bullets:\n"
    "            if target_col:\n"
    "                target_bullets = [\n"
    "                    f\"Primary target metric candidate identified as '{target_col}'.\",\n"
    "                    'Secondary predictive drivers include business size tier, industry classification, and location attributes.'\n"
    "                ]\n"
    "            else:\n"
    "                target_bullets = [\n"
    "                    'No target column was identified. Upload a labeled dataset to enable predictive analysis.',\n"
    "                    'Feature importance is shown relative to all columns without a defined target.'\n"
    "                ]\n"
    "\n"
    "        slides.append({\n"
    "            'id': _uid(),\n"
    "            'type': 'insight',\n"
    "            'category': 'Predictive Intelligence',\n"
    "            'title': 'Key Predictive Drivers',\n"
    "            'subtitle': slide8_subtitle,\n"
    "            'content': '\\n'.join(f'\\u2022 {b}' for b in target_bullets),\n"
    "            'bullets': target_bullets,\n"
    "            'badge': 'TARGET ANALYSIS',\n"
    "            'tags': ['Machine Learning', 'Feature Importance'],\n"
    "            'target_column': target_col,\n"
    "            'is_heuristic': _is_heuristic,\n"
    "            'is_user_defined': _is_user_defined,\n"
    "        })\n"
    "\n"
    "        "
)

result = src[:start] + new_block + src[end:]

# 4b. Add POST /api/story/target endpoint before /export
old_export_line = '\n@router.post("/export")\nasync def export_story'
new_target_ep = '''

class TargetOverrideRequest(BaseModel):
    file_id: str
    target_column: str


@router.post("/target")
async def set_story_target(request: TargetOverrideRequest):
    """
    Override the heuristic target column with an explicit user selection.

    Once called, all downstream modules (ml_readiness, story slide 8,
    feature_ranker) will use this user-confirmed target instead of the
    heuristic candidate, satisfying requirement: every module reads the
    same authoritative target state.
    """
    from state import get_df, set_user_target
    df = get_df(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    try:
        state = set_user_target(request.file_id, request.target_column, df.columns.tolist())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "status": "ok",
        "message": f"Target column set to '{request.target_column}'. All downstream analyses will use this target.",
        "target_state": state,
    }


@router.post("/export")
async def export_story'''

if '\n@router.post("/export")\nasync def export_story' not in result:
    raise RuntimeError("/export marker not found in story.py after slide 8 replacement")
result = result.replace('\n@router.post("/export")\nasync def export_story', new_target_ep, 1)

with open(path, 'w', encoding='utf-8', newline='\r\n') as f:
    f.write(result)

print("OK: story.py patched")
content_check = open(path, encoding='utf-8').read()
assert 'get_target_state' in content_check, "FAIL: story.py missing get_target_state"
assert 'set_user_target' in content_check, "FAIL: story.py missing set_user_target"
assert 'slide8_subtitle' in content_check, "FAIL: story.py missing slide8_subtitle"
assert 'TargetOverrideRequest' in content_check, "FAIL: story.py missing TargetOverrideRequest"
print("All story.py assertions passed.")
