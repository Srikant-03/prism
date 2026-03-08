import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

from profiling.cross_column_models import TemporalAnalysis, TimeSeriesComponent
from profiling.profiling_models import DatasetProfile

class TemporalAnalyzer:
    """
    Detects higher-order temporal patterns matching datetime columns
    with numeric and categorical behaviors over time.
    """

    def analyze(self, df: pd.DataFrame, dataset_profile: DatasetProfile) -> TemporalAnalysis:
        time_keywords = ['year', 'date', 'month', 'timestamp', 'period']
        dt_cols = [
            c.name for c in dataset_profile.columns 
            if (c.semantic_type == 'datetime' or any(k in c.name.lower() for k in time_keywords))
            and c.name in df.columns
        ]
        
        if not dt_cols or len(df) < 50:
            return TemporalAnalysis(has_temporal_patterns=False)

        # Pick the most complete datetime column as the primary index
        primary_dt = None
        min_nulls = float('inf')
        for col in dt_cols:
            nulls = df[col].isnull().sum()
            if nulls < min_nulls:
                min_nulls = nulls
                primary_dt = col

        if not primary_dt or df[primary_dt].notna().sum() < 50:
            return TemporalAnalysis(has_temporal_patterns=False)

        df_t = df.copy()
        s_str = df_t[primary_dt].astype(str).str.strip()
        # Check if it's just a 4-digit year format (e.g. 2015)
        if s_str.str.match(r'^(19|20)\d{2}$').mean() > 0.8:
            df_t[primary_dt] = pd.to_datetime(s_str, format='%Y', errors='coerce')
        else:
            df_t[primary_dt] = pd.to_datetime(df_t[primary_dt], errors='coerce')
            
        df_t = df_t.dropna(subset=[primary_dt]).sort_values(by=primary_dt)

        # Check if we have consistent frequency (approx)
        time_diffs = df_t[primary_dt].diff().dt.total_seconds().dropna()
        if time_diffs.empty:
            return TemporalAnalysis(has_temporal_patterns=False)

        median_diff = time_diffs.median()
        if median_diff == 0:
            # Not a true time-series, just timestamped events with duplicates
            # Need to aggregate.
            df_t = df_t.groupby(primary_dt).mean(numeric_only=True)
            if len(df_t) < 50:
                return TemporalAnalysis(has_temporal_patterns=False)
        else:
            df_t = df_t.set_index(primary_dt)

        # Resample to regular frequency if possible, else just use the sequence
        # For simplicity in auto-profiling, we'll just run STL directly on the sorted numeric indices
        # if the regular index is strict, otherwise we take the raw sequence.

        num_cols = [c.name for c in dataset_profile.columns 
                    if c.semantic_type in ('numeric_continuous', 'currency', 'percentage') 
                    and c.name in df_t.columns]

        decompositions = {}
        periodicities = []

        # Analyze up to 3 numeric columns
        for col in num_cols[:3]:
            try:
                s = df_t[col].dropna()
                if len(s) < 50:
                    continue
                
                # Assume a periodicity (e.g., 7 for weekly, 12 for monthly). 
                # Without strict regular frequency, STL is an approximation here.
                # A robust approach resamples. We will just use period=7 as a basic guess 
                # for demonstration of structural components.
                
                # Resample to a fixed frequency (e.g., daily) to use STL properly:
                # Get the range
                date_range = s.index.max() - s.index.min()
                if date_range.days > 70:
                    # Daily resample
                    s_res = s.resample('D').mean().interpolate(method='linear')
                    period = 7 # Weekly seasonality
                elif date_range.days > 1:
                    # Hourly resample
                    s_res = s.resample('H').mean().interpolate(method='linear')
                    period = 24 # Daily seasonality
                else:
                    s_res = s.values
                    period = max(7, len(s) // 20)

                # Cap resampled data to 500 points to prevent STL hanging
                if hasattr(s_res, '__len__') and len(s_res) > 500:
                    step = len(s_res) // 500
                    s_res = s_res[::step]

                # Need at least 2 periods for STL
                if len(s_res) >= 2 * period:
                    # Use period if it's odd, otherwise period+1 for STL parameters
                    stl = STL(s_res, period=period, robust=True)
                    res = stl.fit()

                    # To send to frontend, we just send a truncated/sampled version of the arrays
                    # Max 100 points
                    if len(s_res) > 200:
                        step = len(s_res) // 100
                        t = res.trend[::step].tolist()
                        season = res.seasonal[::step].tolist()
                        resid = res.resid[::step].tolist()
                        
                        if hasattr(s_res.index, 'strftime'):
                            idx = s_res.index[::step].strftime('%Y-%m-%dT%H:%M:%S').tolist()
                        else:
                            idx = [str(i) for i in range(len(t))]
                    else:
                        t = res.trend.tolist()
                        season = res.seasonal.tolist()
                        resid = res.resid.tolist()
                        if hasattr(s_res.index, 'strftime'):
                            idx = s_res.index.strftime('%Y-%m-%dT%H:%M:%S').tolist()
                        else:
                            idx = [str(i) for i in range(len(t))]

                    # Replace NaNs
                    def clean(arr):
                        return [float(x) if not np.isnan(x) else 0.0 for x in arr]

                    decompositions[col] = TimeSeriesComponent(
                        trend=clean(t),
                        seasonal=clean(season),
                        residual=clean(resid),
                        timestamps=idx
                    )

            except Exception as e:
                pass

        return TemporalAnalysis(
            has_temporal_patterns=len(decompositions) > 0,
            primary_time_col=primary_dt,
            decompositions=decompositions,
            detected_periodicities=periodicities
        )
