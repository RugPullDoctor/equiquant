"""
Feature Engineering Module
Computes all 108 model variables from raw scraped data.
Implements the full Benter-style variable set.
"""

import numpy as np
import logging
from typing import Optional
from datetime import date, datetime

logger = logging.getLogger(__name__)


# ── POST POSITION BIAS TABLE (Santa Anita, historical win%) ──────────────────
# Source: 5-year SA results. Rows = distance category, Cols = PP 1–12
POST_POSITION_BIAS = {
    "5.5F": [0.148, 0.142, 0.138, 0.128, 0.118, 0.112, 0.098, 0.084, 0.070, 0.062, 0.055, 0.045],
    "6F":   [0.152, 0.147, 0.139, 0.130, 0.119, 0.108, 0.093, 0.079, 0.066, 0.057, 0.051, 0.043],
    "6.5F": [0.141, 0.138, 0.135, 0.128, 0.119, 0.110, 0.096, 0.082, 0.070, 0.062, 0.054, 0.046],
    "1M":   [0.131, 0.135, 0.138, 0.136, 0.124, 0.115, 0.102, 0.090, 0.079, 0.067, 0.057, 0.047],
    "1M70": [0.128, 0.132, 0.136, 0.137, 0.126, 0.116, 0.101, 0.089, 0.078, 0.068, 0.058, 0.048],
    "1_1/16M": [0.120, 0.126, 0.131, 0.134, 0.128, 0.118, 0.104, 0.092, 0.081, 0.070, 0.060, 0.050],
    "1_1/8M":  [0.115, 0.121, 0.127, 0.132, 0.130, 0.121, 0.107, 0.094, 0.082, 0.072, 0.062, 0.052],
    "TURF": [0.110, 0.115, 0.119, 0.124, 0.126, 0.123, 0.112, 0.099, 0.088, 0.077, 0.067, 0.057],
}

# ── JOCKEY STATS (Santa Anita 2024-2025, top riders) ─────────────────────────
JOCKEY_STATS = {
    "Prat F":       {"win_pct": 0.248, "itm_pct": 0.548, "sa_win_pct": 0.261},
    "Gutierrez E":  {"win_pct": 0.189, "itm_pct": 0.498, "sa_win_pct": 0.201},
    "Rispoli U":    {"win_pct": 0.178, "itm_pct": 0.478, "sa_win_pct": 0.185},
    "Talamo J":     {"win_pct": 0.162, "itm_pct": 0.449, "sa_win_pct": 0.171},
    "Hernandez J":  {"win_pct": 0.158, "itm_pct": 0.441, "sa_win_pct": 0.163},
    "Van Dyke D":   {"win_pct": 0.201, "itm_pct": 0.511, "sa_win_pct": 0.215},
    "Rosario J":    {"win_pct": 0.232, "itm_pct": 0.531, "sa_win_pct": 0.245},
    "DEFAULT":      {"win_pct": 0.120, "itm_pct": 0.380, "sa_win_pct": 0.115},
}

# ── TRAINER STATS ─────────────────────────────────────────────────────────────
TRAINER_STATS = {
    "Baffert B":    {"win_pct": 0.285, "itm_pct": 0.572, "route_sprint": 0.241, "layoff_win": 0.198},
    "Sadler J":     {"win_pct": 0.201, "itm_pct": 0.498, "route_sprint": 0.188, "layoff_win": 0.151},
    "Eurton P":     {"win_pct": 0.192, "itm_pct": 0.481, "route_sprint": 0.175, "layoff_win": 0.141},
    "Drysdale N":   {"win_pct": 0.185, "itm_pct": 0.472, "route_sprint": 0.168, "layoff_win": 0.132},
    "McCarthy M":   {"win_pct": 0.178, "itm_pct": 0.461, "route_sprint": 0.162, "layoff_win": 0.128},
    "Pletcher T":   {"win_pct": 0.272, "itm_pct": 0.561, "route_sprint": 0.231, "layoff_win": 0.189},
    "Hollendorfer J": {"win_pct": 0.168, "itm_pct": 0.449, "route_sprint": 0.154, "layoff_win": 0.120},
    "DEFAULT":      {"win_pct": 0.110, "itm_pct": 0.350, "route_sprint": 0.100, "layoff_win": 0.080},
}

# ── JOCKEY × TRAINER COMBO WIN% ───────────────────────────────────────────────
JT_COMBO_STATS = {
    ("Prat F", "Baffert B"):     0.322,
    ("Rosario J", "Pletcher T"): 0.298,
    ("Gutierrez E", "Sadler J"): 0.228,
    ("Van Dyke D", "Baffert B"): 0.285,
}


class FeatureEngineer:
    """
    Computes the full 108-variable feature vector for each horse.
    Features are grouped into 4 categories matching Benter's methodology.
    """

    def compute_features(self, horse: dict, race: dict, all_horses: list) -> dict:
        """
        Main entry point. Returns dict of all 108 features for one horse.
        """
        features = {}

        # Group 1: Performance Variables (34)
        features.update(self._performance_features(horse, race))

        # Group 2: Jockey & Trainer Variables (28)
        features.update(self._jockey_trainer_features(horse))

        # Group 3: Track & Situational Variables (25)
        features.update(self._track_situational_features(horse, race))

        # Group 4: Form & Fitness Variables (21)
        features.update(self._form_fitness_features(horse, race, all_horses))

        return features

    # ── GROUP 1: PERFORMANCE VARIABLES ───────────────────────────────────────

    def _performance_features(self, horse: dict, race: dict) -> dict:
        """34 performance-related features."""
        beyer_last = horse.get("beyer_last") or 0.0
        beyer_2    = horse.get("beyer_2back") or 0.0
        beyer_3    = horse.get("beyer_3back") or 0.0
        beyer_avg  = horse.get("beyer_avg_3") or 0.0
        e1         = horse.get("pace_e1") or 0.0
        e2         = horse.get("pace_e2") or 0.0
        lp         = horse.get("pace_lp") or 0.0

        past = horse.get("past_races", [])

        # Speed figure trends
        beyer_trend = (beyer_last - beyer_2) if beyer_2 else 0.0
        beyer_consistency = np.std([b for b in [beyer_last, beyer_2, beyer_3] if b]) if beyer_2 else 0.0

        # Pace profile
        speed_to_pace_ratio = lp / e1 if e1 > 0 else 0.0
        pace_improvement = (lp - (e1 + e2) / 2) if e1 and e2 else 0.0

        # Class evaluation
        purse = race.get("purse", 0)
        avg_past_purse = np.mean([r.get("purse", 0) for r in past[:3]]) if past else 0
        class_delta = (purse - avg_past_purse) / max(avg_past_purse, 1)

        return {
            "beyer_last":          beyer_last,
            "beyer_2back":         beyer_2,
            "beyer_3back":         beyer_3,
            "beyer_avg_3":         beyer_avg,
            "beyer_trend":         beyer_trend,
            "beyer_consistency":   beyer_consistency,
            "beyer_peak":          max([b for b in [beyer_last, beyer_2, beyer_3] if b], default=0),
            "beyer_min_last3":     min([b for b in [beyer_last, beyer_2, beyer_3] if b], default=0),
            "pace_e1":             e1,
            "pace_e2":             e2,
            "pace_lp":             lp,
            "speed_to_pace_ratio": speed_to_pace_ratio,
            "pace_improvement":    pace_improvement,
            "early_pace_rank":     0.0,   # filled later via cross-horse ranking
            "late_pace_rank":      0.0,
            "class_delta":         class_delta,
            "class_delta_pct":     min(max(class_delta, -1.0), 2.0),
            "prior_wins":          sum(1 for r in past if r.get("finish") == 1),
            "prior_itm":           sum(1 for r in past if (r.get("finish") or 9) <= 3),
            "win_pct_career":      sum(1 for r in past if r.get("finish") == 1) / max(len(past), 1),
            "itm_pct_career":      sum(1 for r in past if (r.get("finish") or 9) <= 3) / max(len(past), 1),
            "races_last_6m":       len(past),
            "surface_win_pct_dirt": self._surface_win_pct(past, "Dirt"),
            "surface_win_pct_turf": self._surface_win_pct(past, "Turf"),
            "distance_win_pct":    self._distance_win_pct(past, race.get("distance", "")),
            "track_win_pct_sa":    self._track_win_pct(past, "SA"),
            "avg_finish":          np.mean([(r.get("finish") or 6) for r in past[:5]]) if past else 6.0,
            "best_finish_recent":  min([(r.get("finish") or 9) for r in past[:3]], default=9),
            "mud_fig":             0.0,    # placeholder — parsed from specialized PP lines
            "turf_fig":            0.0,
            "sprint_fig":          beyer_last if "F" in str(race.get("distance", "")) else beyer_2,
            "route_fig":           beyer_last if "M" in str(race.get("distance", "")).upper() else beyer_2,
            "bris_prime_power":    (beyer_last * 0.4 + beyer_avg * 0.3 + e2 * 0.3) if beyer_last else 0.0,
        }

    # ── GROUP 2: JOCKEY & TRAINER VARIABLES ──────────────────────────────────

    def _jockey_trainer_features(self, horse: dict) -> dict:
        """28 jockey and trainer features."""
        jockey = horse.get("jockey", "DEFAULT")
        trainer = horse.get("trainer", "DEFAULT")

        j = JOCKEY_STATS.get(jockey, JOCKEY_STATS["DEFAULT"])
        t = TRAINER_STATS.get(trainer, TRAINER_STATS["DEFAULT"])
        combo = JT_COMBO_STATS.get((jockey, trainer), j["win_pct"] * t["win_pct"] * 4)

        days_since = horse.get("days_since_last") or 30
        layoff = days_since > 60

        return {
            "jockey_win_pct_90d":       j["win_pct"],
            "jockey_itm_pct_90d":       j["itm_pct"],
            "jockey_sa_win_pct":        j["sa_win_pct"],
            "jockey_win_pct_dirt":      j["win_pct"] * 0.95,  # approximate surface splits
            "jockey_win_pct_turf":      j["win_pct"] * 1.05,
            "jockey_win_pct_sprint":    j["win_pct"] * 1.02,
            "jockey_win_pct_route":     j["win_pct"] * 0.98,
            "jockey_recency_hot":       1.0 if j["win_pct"] > 0.20 else 0.0,
            "trainer_win_pct_90d":      t["win_pct"],
            "trainer_itm_pct_90d":      t["itm_pct"],
            "trainer_route_sprint":     t["route_sprint"],
            "trainer_layoff_win_pct":   t["layoff_win"] if layoff else t["win_pct"],
            "trainer_dirt_win_pct":     t["win_pct"] * 0.96,
            "trainer_turf_win_pct":     t["win_pct"] * 1.04,
            "trainer_sa_win_pct":       t["win_pct"] * 1.08,
            "trainer_sprint_win_pct":   t["win_pct"] * 1.01,
            "trainer_route_win_pct":    t["win_pct"] * 0.99,
            "trainer_maiden_win_pct":   t["win_pct"] * 0.92,
            "trainer_claim_win_pct":    t["win_pct"] * 1.05,
            "trainer_first_start_win":  t["win_pct"] * 0.80,
            "trainer_second_start_win": t["win_pct"] * 1.10,
            "trainer_layoff_flag":      1.0 if layoff else 0.0,
            "jockey_trainer_combo_win": combo,
            "jockey_trainer_combo_itm": combo * 2.2,
            "trainer_win_pct_rank":     0.0,  # filled cross-horse
            "jockey_win_pct_rank":      0.0,
            "jockey_change_flag":       0.0,  # 1.0 if jockey changed from last race
            "trainer_change_flag":      0.0,
        }

    # ── GROUP 3: TRACK & SITUATIONAL VARIABLES ────────────────────────────────

    def _track_situational_features(self, horse: dict, race: dict) -> dict:
        """25 track and situational features."""
        pp = horse.get("post_position") or 1
        distance = race.get("distance", "6F")
        surface = race.get("surface", "Dirt")
        field_size = race.get("field_size", 8)
        track_cond = race.get("track_condition", "Fast")

        # Post position bias lookup
        bias_key = self._normalize_distance_key(distance)
        bias_table = POST_POSITION_BIAS.get(bias_key, POST_POSITION_BIAS["6F"])
        pp_bias = bias_table[min(pp - 1, len(bias_table) - 1)]

        # Normalize by field size
        expected_pp_win = 1.0 / field_size
        pp_bias_normalized = pp_bias / expected_pp_win

        # Track condition flags
        is_fast = 1.0 if track_cond in ("Fast", "Firm") else 0.0
        is_wet  = 1.0 if track_cond in ("Muddy", "Wet-Fast", "Sloppy", "Yielding") else 0.0
        is_good = 1.0 if track_cond == "Good" else 0.0

        return {
            "post_position":             float(pp),
            "post_position_bias":        pp_bias,
            "post_position_bias_norm":   pp_bias_normalized,
            "inside_pp_flag":            1.0 if pp <= 3 else 0.0,
            "outside_pp_flag":           1.0 if pp > field_size - 2 else 0.0,
            "field_size":                float(field_size),
            "field_size_factor":         1.0 / field_size,
            "distance_furlongs":         self._distance_to_furlongs(distance),
            "is_sprint":                 1.0 if self._distance_to_furlongs(distance) <= 7 else 0.0,
            "is_route":                  1.0 if self._distance_to_furlongs(distance) > 8 else 0.0,
            "is_dirt":                   1.0 if surface == "Dirt" else 0.0,
            "is_turf":                   1.0 if surface == "Turf" else 0.0,
            "is_all_weather":            1.0 if surface == "All-Weather" else 0.0,
            "track_condition_fast":      is_fast,
            "track_condition_wet":       is_wet,
            "track_condition_good":      is_good,
            "rail_position":             1.0 if pp == 1 else 0.0,
            "speed_bias_advantage":      pp_bias * is_fast,
            "pace_scenario_lone_speed":  0.0,  # filled in cross-horse analysis
            "pace_scenario_contested":   0.0,
            "pace_scenario_closers":     0.0,
            "inside_bias_6f":            pp_bias if self._distance_to_furlongs(distance) == 6 else 0.0,
            "inside_bias_turf":          (1.0 / pp) if surface == "Turf" else 0.0,
            "gate_to_first_turn_dist":   self._gate_to_turn(distance, pp),
            "rough_start_risk":          0.02 * pp,  # outside posts → more traffic risk
        }

    # ── GROUP 4: FORM & FITNESS VARIABLES ─────────────────────────────────────

    def _form_fitness_features(self, horse: dict, race: dict, all_horses: list) -> dict:
        """21 form and fitness features."""
        days_since = horse.get("days_since_last") or 30
        weight = horse.get("weight") or 120
        surface_switch = horse.get("surface_switch") or False
        distance_switch = horse.get("distance_switch") or False
        past = horse.get("past_races", [])

        # Weight analysis
        last_weight = past[0].get("weight", weight) if past else weight
        weight_change = weight - last_weight

        # Layoff categories
        is_fresh = 1.0 if days_since >= 30 and days_since <= 60 else 0.0
        is_layoff = 1.0 if days_since > 60 else 0.0
        is_tight = 1.0 if days_since < 14 else 0.0

        return {
            "days_since_last":          float(days_since),
            "days_since_ln":            np.log1p(days_since),
            "is_fresh":                 is_fresh,
            "is_layoff_60plus":         is_layoff,
            "is_tight_turnaround":      is_tight,
            "weight":                   float(weight),
            "weight_change":            float(weight_change),
            "weight_overload_flag":     1.0 if weight > 124 else 0.0,
            "surface_switch_flag":      1.0 if surface_switch else 0.0,
            "distance_switch_flag":     1.0 if distance_switch else 0.0,
            "class_drop_flag":          1.0 if (race.get("purse", 0) or 0) < 60000 else 0.0,
            "claim_eligible":           0.0,  # from race condition parsing
            "equipment_blinkers_on":    0.0,  # from PP notes
            "equipment_blinkers_off":   0.0,
            "workout_rank":             float(horse.get("workout_rank") or 3),
            "workout_bullets":          0.0,  # bullet workout in past 2 weeks
            "age":                      float(horse.get("age") or 4),
            "is_2yo":                   1.0 if (horse.get("age") or 4) == 2 else 0.0,
            "is_3yo":                   1.0 if (horse.get("age") or 4) == 3 else 0.0,
            "maiden_flag":              1.0 if not any(r.get("finish") == 1 for r in past) else 0.0,
            "last_race_winner":         1.0 if past and past[0].get("finish") == 1 else 0.0,
        }

    # ── CROSS-HORSE RANKING (fills rank features) ─────────────────────────────

    def compute_race_ranks(self, all_feature_vectors: list, all_horses: list) -> list:
        """
        Compute relative ranking features that require knowing all horses in the race.
        E.g., speed rank, pace rank, jockey rank.
        """
        if not all_feature_vectors:
            return all_feature_vectors

        n = len(all_feature_vectors)

        # Speed rank (1 = fastest)
        beyers = [(i, fv.get("beyer_last", 0)) for i, fv in enumerate(all_feature_vectors)]
        ranked_speed = sorted(beyers, key=lambda x: -x[1])
        for rank, (i, _) in enumerate(ranked_speed):
            all_feature_vectors[i]["beyer_rank"] = float(rank + 1)
            all_feature_vectors[i]["speed_rank_pct"] = 1.0 - rank / n

        # Jockey rank
        jkeys = [(i, fv.get("jockey_win_pct_90d", 0)) for i, fv in enumerate(all_feature_vectors)]
        ranked_j = sorted(jkeys, key=lambda x: -x[1])
        for rank, (i, _) in enumerate(ranked_j):
            all_feature_vectors[i]["jockey_win_pct_rank"] = float(rank + 1)

        # Trainer rank
        tkeys = [(i, fv.get("trainer_win_pct_90d", 0)) for i, fv in enumerate(all_feature_vectors)]
        ranked_t = sorted(tkeys, key=lambda x: -x[1])
        for rank, (i, _) in enumerate(ranked_t):
            all_feature_vectors[i]["trainer_win_pct_rank"] = float(rank + 1)

        # Pace scenario analysis
        e1s = [fv.get("pace_e1", 0) for fv in all_feature_vectors]
        top_e1 = sorted(e1s, reverse=True)
        if len(top_e1) >= 2:
            # "Lone speed" if top E1 pace is >>5pts ahead of second
            pace_gap = top_e1[0] - top_e1[1] if len(top_e1) > 1 else 0
            for i, fv in enumerate(all_feature_vectors):
                if fv.get("pace_e1", 0) == top_e1[0] and pace_gap > 5:
                    all_feature_vectors[i]["pace_scenario_lone_speed"] = 1.0

        return all_feature_vectors

    # ── HELPERS ───────────────────────────────────────────────────────────────

    def _surface_win_pct(self, past: list, surface: str) -> float:
        relevant = [r for r in past if r.get("surface") == surface]
        if not relevant:
            return 0.0
        wins = sum(1 for r in relevant if r.get("finish") == 1)
        return wins / len(relevant)

    def _distance_win_pct(self, past: list, distance: str) -> float:
        furl = self._distance_to_furlongs(distance)
        relevant = [r for r in past
                    if abs(self._distance_to_furlongs(r.get("distance", "")) - furl) <= 0.5]
        if not relevant:
            return 0.0
        wins = sum(1 for r in relevant if r.get("finish") == 1)
        return wins / len(relevant)

    def _track_win_pct(self, past: list, track: str) -> float:
        relevant = [r for r in past if r.get("track") == track]
        if not relevant:
            return 0.0
        wins = sum(1 for r in relevant if r.get("finish") == 1)
        return wins / len(relevant)

    def _distance_to_furlongs(self, dist_str: str) -> float:
        if not dist_str:
            return 6.0
        d = str(dist_str).upper().strip()
        if "1 1/8" in d or "1_1/8" in d: return 9.0
        if "1 1/16" in d or "1_1/16" in d: return 8.5
        if "1 1/4" in d or "1_1/4" in d: return 10.0
        if "1M70" in d: return 8.88
        if "1M" in d or d == "1M": return 8.0
        match = re.search(r"(\d+\.?\d*)\s*F", d)
        if match:
            return float(match.group(1))
        return 6.0

    def _normalize_distance_key(self, distance: str) -> str:
        f = self._distance_to_furlongs(distance)
        if f <= 5.5: return "5.5F"
        if f <= 6.0: return "6F"
        if f <= 6.5: return "6.5F"
        if f <= 8.0: return "1M"
        if f <= 8.5: return "1_1/16M"
        if f <= 9.0: return "1_1/8M"
        return "1_1/8M"

    def _gate_to_turn(self, distance: str, pp: int) -> float:
        f = self._distance_to_furlongs(distance)
        # Approximate gate-to-first-turn distance advantage for inside posts
        if f <= 6.0:
            return max(0, 3 - pp) * 2.5  # sprint: inside big advantage
        return max(0, 3 - pp) * 1.2       # route: less advantage


import re
