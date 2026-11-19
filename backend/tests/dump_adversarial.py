import sys
import os
sys.path.insert(0, os.path.abspath('backend'))

import json
import pandas as pd
from profiling.engine import DataProfiler
from profiling.formula_detector import detect_formula_dependencies
from profiling.target_detector import TargetDetector
from cleaning.missing_handler import MissingHandler
from cleaning.type_handler import TypeHandler
from cleaning.outlier_handler import OutlierHandler
from dashboard.semantic_model import build_semantic_model
from dashboard.ai_analytics import compute_key_influencers, run_forecast, generate_smart_narrative

df = pd.read_csv('prism_adversarial_e2e_test.csv')
profiling_res = DataProfiler.profile(df, 'adversarial')
p = profiling_res.profile

formulas = detect_formula_dependencies(df)
td = TargetDetector().analyze(df, p, p.cross_analysis.get('correlations'))
sm = build_semantic_model('adversarial', df)
ki = compute_key_influencers(df, 'churn_flag', 1)
fc = run_forecast(df, 'order_date', 'net_revenue', 'month', 4)
sn = generate_smart_narrative(df)

mh = MissingHandler(df, 'adversarial')
m_actions, m_report = mh.analyze()

th = TypeHandler(df, 'adversarial')
t_actions, t_report = th.analyze()

oh = OutlierHandler(df, 'adversarial')
o_actions, o_report = oh.analyze()

out = {
    'shape': list(df.shape),
    'null_count': int(df.isna().sum().sum()),
    'completeness': p.structural_completeness,
    'domain': p.estimated_domain,
    'domain_confidence': p.domain_confidence,
    'columns': [
        {
            'name': c.name,
            'semantic_type': c.semantic_type,
            'null_count': c.null_count,
            'null_pct': c.null_percentage,
            'quality_score': c.quality_score,
            'unique_count': c.distinct_count,
        }
        for c in p.columns
    ],
    'formulas': [
        {
            'dependent': f.dependent,
            'operands': f.operands,
            'formula_expr': f.formula_expr,
            'description': f.description,
            'match_ratio': f.match_ratio,
            'sample_size': f.sample_size,
        }
        for f in formulas
    ],
    'target': {
        'target_column': td.target_column,
        'problem_type': td.problem_type,
        'confidence': td.confidence,
        'distribution': td.class_distribution,
    },
    'cleaning': {
        'high_null_rows': m_report.high_null_rows,
        'missing_actions': [a.recommendation for a in m_actions],
        'type_actions': [a.recommendation for a in t_actions],
        'outlier_actions': [a.recommendation for a in o_actions],
    },
    'semantic_model': {
        'measures': [m.name for m in sm.measures],
        'categoricals': sm.categorical_columns,
        'numerics': sm.numeric_columns,
        'hierarchies': sm.hierarchies,
    },
    'key_influencers': {
        'guarded': ki.guarded_leakage_columns,
        'baseline_rate': ki.baseline_rate,
        'drivers': [
            {'feature': d.feature, 'condition': d.condition, 'impact': d.impact, 'multiplier': d.multiplier, 'segment_size': d.segment_size, 'target_rate': d.target_rate}
            for d in ki.drivers
        ],
    },
    'forecasting': {
        'model': fc.model_name,
        'trend': fc.trend,
        'points_count': len(fc.points),
        'anomalies_count': fc.anomalies_detected,
    },
    'smart_narrative': {
        'title': sn.title,
        'bullets': sn.bullets,
        'kpis': sn.summary_kpis,
    }
}

with open('backend/tests/adversarial_e2e_results_dump.json', 'w') as fp:
    json.dump(out, fp, indent=2, default=str)

print('SUCCESSFULLY_DUMPED_RESULTS')
