import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

INITIAL_WEIGHTS = {
    "beyer_last":               0.048,
    "beyer_avg_3":              0.031,
    "beyer_trend":              0.022,
    "pace_e1":                  0.018,
    "pace_lp":                  0.025,
    "bris_prime_power":         0.039,
    "speed_rank_pct":           0.082,
    "jockey_win_pct_90d":       1.240,
    "jockey_sa_win_pct":        0.890,
    "trainer_win_pct_90d":      1.180,
    "jockey_trainer_combo_win": 1.540,
    "post_position_bias":       2.100,
    "inside_pp_flag":           0.145,
    "track_condition_fast":     0.082,
    "field_size_factor":        0.210,
    "days_since_last":         -0.004,
    "is_fresh":                 0.088,
    "is_layoff_60plus":        -0.121,
    "surface_switch_flag":     -0.142,
    "win_pct_career":           0.610,
    "itm_pct_career":           0.420,
    "last_race_winner":         0.195,
    "pace_scenario_lone_speed": 0.312,
}


class BenterModel:
    def __init__(self):
        self.weights = INITIAL_WEIGHTS.copy()
        self.intercept = 0.0

    def score_horse(self, features: dict) -> float:
        score = self.intercept
        for feat, beta in self.weights.items():
            val = features.get(feat, 0.0) or 0.0
            score += beta * float(val)
        return score

    def predict_race(self, feature_vectors: list) -> list:
        if not feature_vectors:
            return []
        scores = [self.score_horse(fv) for fv in feature_vectors]
        max_score = max(scores)
        exp_scores = [np.exp(s - max_score) for s in scores]
        total = sum(exp_scores)
        return [e / total for e in exp_scores]

    def compute_edge(self, model_prob: float, track_odds: str) -> float:
        return model_prob - self._odds_to_prob(track_odds)

    def kelly_fraction(self, model_prob: float, track_odds: str) -> float:
        decimal_odds = self._odds_to_decimal(track_odds)
        b = decimal_odds - 1.0
        p = model_prob
        q = 1.0 - p
        f = (b * p - q) / b if b > 0 else 0.0
        return max(0.0, f)

    def bet_size(self, model_prob, track_odds, bankroll, kelly_fraction=0.25, max_bet=25000.0):
        edge = self.compute_edge(model_prob, track_odds)
        if edge <= 0:
            return 0.0
        f = self.kelly_fraction(model_prob, track_odds)
        return min(f * kelly_fraction * bankroll, max_bet)

    def analyze_race(self, horses, feature_vectors, bankroll, kelly_frac=0.25):
        probs = self.predict_race(feature_vectors)
        results = []
        for horse, fv, prob in zip(horses, feature_vectors, probs):
            odds = horse.get("live_odds") or horse.get("morning_line_odds") or "9/1"
            edge = self.compute_edge(prob, odds)
            bet = self.bet_size(prob, odds, bankroll, kelly_frac)
            results.append({
                **horse,
                "model_win_prob": round(prob, 4),
                "track_implied_prob": round(self._odds_to_prob(odds), 4),
                "edge": round(edge, 4),
                "edge_pct": round(edge * 100, 2),
                "kelly_bet": round(bet, 2),
                "bet_recommended": edge > 0.04,
            })
        return results

    def _odds_to_prob(self, odds):
        try:
            if "/" in str(odds):
                n, d = odds.split("/")
                return float(d) / (float(n) + float(d))
            return 1.0 / float(odds)
        except Exception:
            return 0.10

    def _odds_to_decimal(self, odds):
        try:
            if "/" in str(odds):
                n, d = odds.split("/")
                return (float(n) + float(d)) / float(d)
            return float(odds)
        except Exception:
            return 10.0
