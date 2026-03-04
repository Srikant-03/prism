import numpy as np
import pandas as pd

from profiling.cross_column_models import GeoAnalysis
from profiling.profiling_models import DatasetProfile

class GeoAnalyzer:
    """
    Detects geographic patterns and aggregates location data for maps.
    """

    def analyze(self, df: pd.DataFrame, dataset_profile: DatasetProfile) -> GeoAnalysis:
        geo_cols = [c.name for c in dataset_profile.columns if c.semantic_type == 'geo_coordinate']
        named_geo_cols = [c.name for c in dataset_profile.columns if any(p in c.name.lower() for p in ('country', 'city', 'state', 'zip', 'postal', 'region', 'location'))]
        
        all_geo = list(set(geo_cols + named_geo_cols))
        
        if not all_geo or len(df) == 0:
            return GeoAnalysis(has_geo_patterns=False)

        # Look for explicit lat/long columns
        lat_cols = [c for c in geo_cols if 'lat' in c.lower()]
        lon_cols = [c for c in geo_cols if 'lon' in c.lower() or 'lng' in c.lower()]

        bounding_box = None
        if lat_cols and lon_cols:
            lat_col = lat_cols[0]
            lon_col = lon_cols[0]
            
            lats = pd.to_numeric(df[lat_col], errors='coerce').dropna()
            lons = pd.to_numeric(df[lon_col], errors='coerce').dropna()
            
            if len(lats) > 0 and len(lons) > 0:
                bounding_box = {
                    'min_lat': float(lats.min()),
                    'max_lat': float(lats.max()),
                    'min_lon': float(lons.min()),
                    'max_lon': float(lons.max())
                }

        # Look for distribution strings (Country usually best)
        geo_distribution = {}
        target_agg_col = [c for c in named_geo_cols if 'country' in c.lower()]
        if not target_agg_col:
            target_agg_col = [c for c in named_geo_cols if 'state' in c.lower() or 'city' in c.lower()]

        if target_agg_col:
            dist_col = target_agg_col[0]
            s = df[dist_col].dropna().astype(str).str.title().str.strip()
            if len(s) > 0:
                counts = s.value_counts()
                # Take top 50 regions to limit JSON size
                geo_distribution = counts.head(50).to_dict()

        return GeoAnalysis(
            has_geo_patterns=True,
            geo_columns=all_geo,
            bounding_box=bounding_box,
            geo_distribution=geo_distribution
        )
