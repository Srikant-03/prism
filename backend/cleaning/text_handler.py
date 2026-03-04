"""
Text Handler — Preprocessing for free-text columns.
Normalization, NLP features, stopwords, stemming, sentiment, TF-IDF.
"""

from __future__ import annotations

import re
import html
from typing import Any, Optional
from collections import Counter

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
    TextFeatureReport, TextColumnAnalysis,
)


# ── HTML / Markdown patterns ─────────────────────────────────────────
_HTML_TAG = re.compile(r"<[^>]+>")
_MARKDOWN_SYNTAX = re.compile(r"[*_~`#\[\]\(\)!]")
_URL_IN_TEXT = re.compile(r"https?://\S+")
_EMOJI_PATTERN = re.compile(
    r"[\U0001F600-\U0001F64F"
    r"\U0001F300-\U0001F5FF"
    r"\U0001F680-\U0001F6FF"
    r"\U0001F900-\U0001F9FF"
    r"\U0001FA00-\U0001FA6F"
    r"\U0001FA70-\U0001FAFF"
    r"\U00002702-\U000027B0"
    r"\U0000FE00-\U0000FE0F"
    r"\U0001F1E0-\U0001F1FF"
    r"]+",
    flags=re.UNICODE,
)

# ── Contractions ──────────────────────────────────────────────────────
_CONTRACTIONS = {
    "don't": "do not", "doesn't": "does not", "didn't": "did not",
    "won't": "will not", "wouldn't": "would not", "couldn't": "could not",
    "shouldn't": "should not", "can't": "cannot", "isn't": "is not",
    "aren't": "are not", "wasn't": "was not", "weren't": "were not",
    "hasn't": "has not", "haven't": "have not", "hadn't": "had not",
    "mustn't": "must not", "shan't": "shall not", "needn't": "need not",
    "i'm": "i am", "you're": "you are", "he's": "he is", "she's": "she is",
    "it's": "it is", "we're": "we are", "they're": "they are",
    "i've": "i have", "you've": "you have", "we've": "we have",
    "they've": "they have", "i'll": "i will", "you'll": "you will",
    "he'll": "he will", "she'll": "she will", "we'll": "we will",
    "they'll": "they will", "i'd": "i would", "you'd": "you would",
    "he'd": "he would", "she'd": "she would", "we'd": "we would",
    "they'd": "they would", "let's": "let us", "that's": "that is",
    "who's": "who is", "what's": "what is", "here's": "here is",
    "there's": "there is", "where's": "where is", "how's": "how is",
}


class TextHandler:
    """Text column analysis and preprocessing engine."""

    def __init__(self, df: pd.DataFrame, file_id: str, profile: Any = None):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], TextFeatureReport]:
        actions: list[CleaningAction] = []
        report = TextFeatureReport()

        text_cols = self._identify_text_columns()
        report.total_text_columns = len(text_cols)

        for col in text_cols:
            analysis = self._analyze_text_column(col)
            report.text_columns.append(analysis)

            # Generate actions based on analysis
            col_actions = self._generate_actions(col, analysis)
            actions.extend(col_actions)

        report.total_features_extractable = sum(
            len(tc.recommended_operations) for tc in report.text_columns
        )

        return actions, report

    # ── Identify free-text columns ────────────────────────────────────
    def _identify_text_columns(self) -> list[str]:
        """Find columns that contain free-text (not categorical/ID)."""
        text_cols: list[str] = []
        for col in self.df.columns:
            if self.df[col].dtype != object:
                continue

            non_null = self.df[col].dropna()
            if len(non_null) == 0:
                continue

            # Heuristics for free text vs categorical
            unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
            avg_len = non_null.astype(str).str.len().mean()
            avg_words = non_null.astype(str).str.split().str.len().mean()

            # Free text: high uniqueness + reasonable avg length
            if unique_ratio > 0.3 and avg_words > 3:
                text_cols.append(col)
            elif avg_len > 50 and avg_words > 5:
                text_cols.append(col)

        return text_cols

    # ── Analyze a single text column ──────────────────────────────────
    def _analyze_text_column(self, col: str) -> TextColumnAnalysis:
        non_null = self.df[col].dropna().astype(str)
        sample = non_null.head(500)

        # Basic stats
        avg_tokens = float(sample.str.split().str.len().mean()) if len(sample) > 0 else 0
        avg_chars = float(sample.str.len().mean()) if len(sample) > 0 else 0
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0

        # Content detection
        has_html = bool(sample.str.contains(_HTML_TAG, na=False).any())
        has_markdown = bool(sample.str.contains(_MARKDOWN_SYNTAX, na=False).mean() > 0.3)
        has_special = bool(sample.str.contains(r'[^\w\s.,!?\-\'"()]', na=False, regex=True).mean() > 0.2)
        has_emojis = bool(sample.str.contains(_EMOJI_PATTERN, na=False).any())
        has_urls = bool(sample.str.contains(_URL_IN_TEXT, na=False).any())

        # Language detection
        lang, lang_conf = self._detect_language(sample)

        # Recommended operations
        ops: list[ActionType] = []
        if has_html:
            ops.append(ActionType.NORMALIZE_TEXT)
        if has_markdown:
            ops.append(ActionType.NORMALIZE_TEXT)
        if avg_tokens > 3:
            ops.append(ActionType.EXTRACT_TEXT_FEATURES)
        if avg_tokens > 10:
            ops.append(ActionType.REMOVE_STOPWORDS)
            ops.append(ActionType.STEM_LEMMATIZE)
        if avg_tokens > 20:
            ops.append(ActionType.TFIDF_VECTORIZE)
        if unique_ratio < 0.5 and avg_tokens < 5:
            ops.append(ActionType.CORRECT_SPELLING)

        # Extract feature preview
        features_preview = self._extract_features_preview(sample.head(5))

        return TextColumnAnalysis(
            column=col,
            avg_token_count=round(avg_tokens, 1),
            avg_char_length=round(avg_chars, 1),
            detected_language=lang,
            language_confidence=lang_conf,
            has_html=has_html,
            has_markdown=has_markdown,
            has_special_chars=has_special,
            has_emojis=has_emojis,
            has_urls=has_urls,
            unique_ratio=round(unique_ratio, 3),
            recommended_operations=ops,
            extracted_features_preview=features_preview,
        )

    # ── Language Detection ────────────────────────────────────────────
    def _detect_language(self, sample: pd.Series) -> tuple[str, float]:
        """Detect dominant language from text sample."""
        try:
            from langdetect import detect_langs
            combined = " ".join(sample.head(50).tolist())
            if len(combined.strip()) < 20:
                return "en", 0.5

            results = detect_langs(combined)
            if results:
                return results[0].lang, round(results[0].prob, 3)
        except Exception:
            pass

        # Fallback: basic heuristic
        return "en", 0.5

    # ── Feature Extraction Preview ────────────────────────────────────
    def _extract_features_preview(self, sample: pd.Series) -> dict[str, Any]:
        """Extract text features for a small sample to preview."""
        if len(sample) == 0:
            return {}

        features: dict[str, list] = {
            "token_count": [],
            "char_count": [],
            "avg_word_length": [],
            "capitalization_ratio": [],
            "digit_ratio": [],
            "punctuation_density": [],
        }

        for text in sample:
            text = str(text)
            tokens = text.split()
            features["token_count"].append(len(tokens))
            features["char_count"].append(len(text))
            features["avg_word_length"].append(
                round(np.mean([len(t) for t in tokens]), 2) if tokens else 0
            )
            upper = sum(1 for c in text if c.isupper())
            features["capitalization_ratio"].append(
                round(upper / max(len(text), 1), 3)
            )
            digits = sum(1 for c in text if c.isdigit())
            features["digit_ratio"].append(
                round(digits / max(len(text), 1), 3)
            )
            punct = sum(1 for c in text if c in ".,;:!?-()[]{}\"'")
            features["punctuation_density"].append(
                round(punct / max(len(text), 1), 3)
            )

        # Sentiment preview (optional)
        try:
            from textblob import TextBlob
            sentiments = []
            subjectivities = []
            for text in sample.head(3):
                blob = TextBlob(str(text))
                sentiments.append(round(blob.sentiment.polarity, 3))
                subjectivities.append(round(blob.sentiment.subjectivity, 3))
            features["sentiment_polarity"] = sentiments
            features["subjectivity_score"] = subjectivities
        except ImportError:
            pass

        return features

    # ── Generate CleaningActions ──────────────────────────────────────
    def _generate_actions(self, col: str, analysis: TextColumnAnalysis) -> list[CleaningAction]:
        actions: list[CleaningAction] = []

        # 1. Normalization action (if HTML/Markdown/special chars detected)
        if analysis.has_html or analysis.has_markdown or analysis.has_special_chars or analysis.has_emojis:
            norm_parts: list[str] = []
            if analysis.has_html:
                norm_parts.append("HTML tags")
            if analysis.has_markdown:
                norm_parts.append("Markdown syntax")
            if analysis.has_special_chars:
                norm_parts.append("special characters")
            if analysis.has_emojis:
                norm_parts.append("emojis")

            actions.append(CleaningAction(
                category=ActionCategory.TEXT_PREPROCESSING,
                action_type=ActionType.NORMALIZE_TEXT,
                confidence=ActionConfidence.JUDGMENT_CALL,
                evidence=f"Column '{col}' contains: {', '.join(norm_parts)}.",
                recommendation=f"Normalize text in '{col}': lowercase, trim whitespace, remove {', '.join(norm_parts)}, expand contractions.",
                reasoning="Text normalization creates a consistent representation for analysis. HTML and Markdown are presentation artifacts not semantic content.",
                target_columns=[col],
                impact=ImpactEstimate(
                    rows_before=self.n_rows, rows_after=self.n_rows,
                    description=f"Normalizes text in {self.n_rows:,} rows.",
                ),
                options=[
                    UserOption(key="full", label="Full Normalization", description="Lowercase + trim + remove HTML/MD/special + expand contractions", is_default=True),
                    UserOption(key="light", label="Light Normalization", description="Lowercase + trim whitespace only"),
                    UserOption(key="html_only", label="Remove HTML/Markdown Only"),
                    UserOption(key="skip", label="Skip"),
                ],
                metadata={
                    "has_html": analysis.has_html,
                    "has_markdown": analysis.has_markdown,
                    "has_emojis": analysis.has_emojis,
                },
            ))

        # 2. Feature extraction action
        if analysis.avg_token_count > 3:
            feature_names = [
                "token_count", "char_count", "sentence_count", "avg_word_length",
                "capitalization_ratio", "digit_ratio", "punctuation_density",
                "detected_language", "sentiment_polarity", "subjectivity_score",
            ]
            actions.append(CleaningAction(
                category=ActionCategory.TEXT_PREPROCESSING,
                action_type=ActionType.EXTRACT_TEXT_FEATURES,
                confidence=ActionConfidence.JUDGMENT_CALL,
                evidence=(
                    f"Column '{col}' is free text (avg {analysis.avg_token_count:.0f} tokens, "
                    f"{analysis.avg_char_length:.0f} chars). Language: {analysis.detected_language}."
                ),
                recommendation=f"Extract {len(feature_names)} numeric features from '{col}'.",
                reasoning="Numeric features enable quantitative analysis of text without NLP models. Features include token/char counts, capitalization patterns, sentiment polarity, and subjectivity.",
                target_columns=[col],
                impact=ImpactEstimate(
                    rows_before=self.n_rows, rows_after=self.n_rows,
                    columns_before=self.n_cols,
                    columns_after=self.n_cols + len(feature_names),
                    columns_affected=len(feature_names),
                    description=f"Adds {len(feature_names)} feature columns from '{col}'.",
                ),
                options=[
                    UserOption(key="all", label=f"Extract All {len(feature_names)} Features", is_default=True),
                    UserOption(key="basic", label="Basic Only (counts + ratios)"),
                    UserOption(key="sentiment", label="Sentiment Only"),
                    UserOption(key="skip", label="Skip"),
                ],
                metadata={"features": feature_names, "language": analysis.detected_language},
            ))

        # 3. Stopword removal + stemming/lemmatization
        if analysis.avg_token_count > 10:
            actions.append(CleaningAction(
                category=ActionCategory.TEXT_PREPROCESSING,
                action_type=ActionType.REMOVE_STOPWORDS,
                confidence=ActionConfidence.JUDGMENT_CALL,
                evidence=f"Column '{col}' contains lengthy text (avg {analysis.avg_token_count:.0f} tokens). Language: {analysis.detected_language}.",
                recommendation=f"Remove stopwords and apply stemming/lemmatization to '{col}'.",
                reasoning=f"Stopword removal reduces noise for text analysis. Language '{analysis.detected_language}' stopwords will be used.",
                target_columns=[col],
                impact=ImpactEstimate(
                    rows_before=self.n_rows, rows_after=self.n_rows,
                    description=f"Processes text in {self.n_rows:,} rows.",
                ),
                options=[
                    UserOption(key="stopwords_lemma", label="Stopwords + Lemmatization", is_default=True),
                    UserOption(key="stopwords_stem", label="Stopwords + Porter Stemming"),
                    UserOption(key="stopwords_only", label="Stopwords Only"),
                    UserOption(key="lemma_only", label="Lemmatization Only"),
                    UserOption(key="skip", label="Skip"),
                ],
            ))

        # 4. TF-IDF vectorization
        if analysis.avg_token_count > 20:
            actions.append(CleaningAction(
                category=ActionCategory.TEXT_PREPROCESSING,
                action_type=ActionType.TFIDF_VECTORIZE,
                confidence=ActionConfidence.JUDGMENT_CALL,
                evidence=f"Column '{col}' contains substantive text (avg {analysis.avg_token_count:.0f} tokens) suitable for vectorization.",
                recommendation=f"Apply TF-IDF vectorization to '{col}' (max 100 features, 1-2 n-grams).",
                reasoning="TF-IDF converts text to numeric vectors capturing word importance. Useful for similarity search, clustering, and machine learning.",
                target_columns=[col],
                impact=ImpactEstimate(
                    rows_before=self.n_rows, rows_after=self.n_rows,
                    columns_before=self.n_cols, columns_after=self.n_cols + 100,
                    columns_affected=100,
                    description="Adds up to 100 TF-IDF feature columns.",
                ),
                options=[
                    UserOption(key="tfidf_100", label="TF-IDF (max 100 features)", is_default=True),
                    UserOption(key="tfidf_50", label="TF-IDF (max 50 features)"),
                    UserOption(key="tfidf_200", label="TF-IDF (max 200 features)"),
                    UserOption(key="skip", label="Skip"),
                ],
                metadata={"max_features": 100, "ngram_range": [1, 2]},
            ))

        # 5. Spelling correction (for short repeated text)
        if analysis.unique_ratio < 0.5 and analysis.avg_token_count < 5:
            actions.append(CleaningAction(
                category=ActionCategory.TEXT_PREPROCESSING,
                action_type=ActionType.CORRECT_SPELLING,
                confidence=ActionConfidence.JUDGMENT_CALL,
                evidence=f"Column '{col}' has low uniqueness ({analysis.unique_ratio:.0%}) with short values — likely categorical text with typos.",
                recommendation=f"Apply spelling correction to standardize values in '{col}'.",
                reasoning="Short text with low uniqueness often contains misspellings of the same values. Correction reduces spurious cardinality.",
                target_columns=[col],
                impact=ImpactEstimate(
                    rows_before=self.n_rows, rows_after=self.n_rows,
                    description=f"Corrects spelling in '{col}'.",
                ),
                options=[
                    UserOption(key="correct", label="Auto-Correct Spelling", is_default=True),
                    UserOption(key="skip", label="Skip"),
                ],
            ))

        # 6. Drop raw text after feature extraction (always last)
        if analysis.avg_token_count > 10:
            actions.append(CleaningAction(
                category=ActionCategory.TEXT_PREPROCESSING,
                action_type=ActionType.DROP_RAW_TEXT,
                confidence=ActionConfidence.JUDGMENT_CALL,
                evidence=f"After feature extraction, the raw text column '{col}' may no longer be needed.",
                recommendation=f"Drop raw text column '{col}' after extracting features.",
                reasoning="Raw text columns are not directly usable by most ML algorithms. After extracting numeric features and/or TF-IDF vectors, the original text can be dropped to reduce memory.",
                target_columns=[col],
                impact=ImpactEstimate(
                    rows_before=self.n_rows, rows_after=self.n_rows,
                    columns_before=self.n_cols, columns_after=self.n_cols - 1,
                    columns_affected=1,
                    description=f"Drops raw text column '{col}'.",
                ),
                options=[
                    UserOption(key="drop", label="Drop Column", is_default=True),
                    UserOption(key="keep", label="Keep Column"),
                ],
            ))

        return actions

    # ── Text Normalization Execution ──────────────────────────────────
    @staticmethod
    def normalize_text(text: str, mode: str = "full") -> str:
        """Apply text normalization."""
        if not isinstance(text, str):
            return str(text)

        # Always: trim whitespace
        text = text.strip()
        text = re.sub(r"\s+", " ", text)

        if mode in ("full", "html_only"):
            # Remove HTML
            text = html.unescape(text)
            text = _HTML_TAG.sub("", text)
            # Remove markdown
            text = re.sub(r"#{1,6}\s*", "", text)
            text = re.sub(r"\*{1,3}(.*?)\*{1,3}", r"\1", text)
            text = re.sub(r"_{1,3}(.*?)_{1,3}", r"\1", text)
            text = re.sub(r"~~(.*?)~~", r"\1", text)
            text = re.sub(r"`{1,3}(.*?)`{1,3}", r"\1", text)
            text = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text)  # [text](url) → text

        if mode == "html_only":
            return text

        if mode in ("full", "light"):
            text = text.lower()

        if mode == "full":
            # Remove emojis
            text = _EMOJI_PATTERN.sub("", text)
            # Remove URLs
            text = _URL_IN_TEXT.sub("", text)
            # Expand contractions
            for contraction, expansion in _CONTRACTIONS.items():
                text = re.sub(r"\b" + re.escape(contraction) + r"\b", expansion, text, flags=re.IGNORECASE)
            # Normalize whitespace again
            text = re.sub(r"\s+", " ", text).strip()

        return text

    # ── Feature Extraction Execution ──────────────────────────────────
    @staticmethod
    def extract_features(series: pd.Series, mode: str = "all") -> pd.DataFrame:
        """Extract numeric features from a text column."""
        texts = series.fillna("").astype(str)
        features = pd.DataFrame(index=series.index)

        # Basic features (always)
        features["token_count"] = texts.str.split().str.len().fillna(0).astype(int)
        features["char_count"] = texts.str.len().fillna(0).astype(int)
        features["avg_word_length"] = texts.apply(
            lambda t: round(np.mean([len(w) for w in t.split()]), 2) if t.split() else 0
        )
        features["capitalization_ratio"] = texts.apply(
            lambda t: round(sum(1 for c in t if c.isupper()) / max(len(t), 1), 3)
        )
        features["digit_ratio"] = texts.apply(
            lambda t: round(sum(1 for c in t if c.isdigit()) / max(len(t), 1), 3)
        )
        features["punctuation_density"] = texts.apply(
            lambda t: round(sum(1 for c in t if c in ".,;:!?-()[]{}\"'") / max(len(t), 1), 3)
        )

        if mode in ("all", "basic"):
            # Sentence count
            features["sentence_count"] = texts.apply(
                lambda t: max(len(re.split(r'[.!?]+', t)) - 1, 0) if t else 0
            )

        if mode in ("all", "sentiment"):
            try:
                from textblob import TextBlob
                features["sentiment_polarity"] = texts.apply(
                    lambda t: round(TextBlob(t).sentiment.polarity, 3)
                )
                features["subjectivity_score"] = texts.apply(
                    lambda t: round(TextBlob(t).sentiment.subjectivity, 3)
                )
            except ImportError:
                features["sentiment_polarity"] = 0.0
                features["subjectivity_score"] = 0.0

        # Language detection (per-row, sampled)
        if mode == "all":
            try:
                from langdetect import detect
                features["detected_language"] = texts.head(len(texts)).apply(
                    lambda t: detect(t) if len(t.strip()) > 10 else "unknown"
                )
            except ImportError:
                features["detected_language"] = "unknown"

        return features

    # ── TF-IDF Execution ──────────────────────────────────────────────
    @staticmethod
    def tfidf_vectorize(series: pd.Series, max_features: int = 100, ngram_range: tuple = (1, 2)) -> pd.DataFrame:
        """Apply TF-IDF vectorization."""
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer

            texts = series.fillna("").astype(str)
            vectorizer = TfidfVectorizer(
                max_features=max_features,
                ngram_range=ngram_range,
                stop_words="english",
                min_df=2,
                max_df=0.95,
            )
            tfidf_matrix = vectorizer.fit_transform(texts)
            feature_names = [f"tfidf_{name}" for name in vectorizer.get_feature_names_out()]
            return pd.DataFrame(
                tfidf_matrix.toarray(),
                columns=feature_names,
                index=series.index,
            )
        except ImportError:
            return pd.DataFrame(index=series.index)

    # ── Stopword / Stemming / Lemmatization Execution ─────────────────
    @staticmethod
    def process_nlp(series: pd.Series, mode: str = "stopwords_lemma", language: str = "en") -> pd.Series:
        """Apply stopword removal and/or stemming/lemmatization."""
        texts = series.fillna("").astype(str)

        try:
            import nltk
            try:
                nltk.data.find('corpora/stopwords')
            except LookupError:
                nltk.download('stopwords', quiet=True)
            try:
                nltk.data.find('corpora/wordnet')
            except LookupError:
                nltk.download('wordnet', quiet=True)
            try:
                nltk.data.find('tokenizers/punkt_tab')
            except LookupError:
                nltk.download('punkt_tab', quiet=True)

            from nltk.corpus import stopwords
            from nltk.stem import PorterStemmer, WordNetLemmatizer
            from nltk.tokenize import word_tokenize

            # Get stopwords for detected language
            lang_map = {"en": "english", "es": "spanish", "fr": "french", "de": "german",
                        "it": "italian", "pt": "portuguese", "nl": "dutch", "sv": "swedish",
                        "no": "norwegian", "da": "danish", "fi": "finnish", "ru": "russian"}
            stop_lang = lang_map.get(language, "english")
            try:
                stop_words = set(stopwords.words(stop_lang))
            except OSError:
                stop_words = set(stopwords.words("english"))

            stemmer = PorterStemmer()
            lemmatizer = WordNetLemmatizer()

            def process_text(text: str) -> str:
                tokens = word_tokenize(text.lower())

                if mode in ("stopwords_lemma", "stopwords_stem", "stopwords_only"):
                    tokens = [t for t in tokens if t not in stop_words and t.isalpha()]

                if mode in ("stopwords_lemma", "lemma_only"):
                    tokens = [lemmatizer.lemmatize(t) for t in tokens]
                elif mode in ("stopwords_stem",):
                    tokens = [stemmer.stem(t) for t in tokens]

                return " ".join(tokens)

            return texts.apply(process_text)

        except ImportError:
            # Fallback: basic stopword removal
            basic_stops = {"the", "a", "an", "is", "are", "was", "were", "of", "in", "to", "for", "and", "or", "but"}

            def basic_process(text: str) -> str:
                tokens = text.lower().split()
                tokens = [t for t in tokens if t not in basic_stops and t.isalpha()]
                return " ".join(tokens)

            return texts.apply(basic_process)

    # ── Spelling Correction Execution ─────────────────────────────────
    @staticmethod
    def correct_spelling(series: pd.Series) -> pd.Series:
        """Correct spelling for short categorical text."""
        try:
            from textblob import TextBlob
            return series.apply(
                lambda t: str(TextBlob(str(t)).correct()) if pd.notna(t) and len(str(t).strip()) > 0 else t
            )
        except ImportError:
            return series
