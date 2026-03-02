"""
Starter pilot generator for the Zomathon Tier A dataset spec.

What it does (v0):
- Reads generation_config YAML
- Generates synthetic tables: restaurants, menu_items, users, sessions, cart_events, reco_exposures
- Writes CSV/Parquet (Parquet if pyarrow installed) + small samples for spreadsheet review

What it does NOT yet do (TODO upgrades):
- Real catalog ingestion (currently synthetic names/tags)
- Distilabel enrichment, DeepEval, Label Studio integration
- Advanced sequential logic, availability churn, temporal drift calibration
"""
from __future__ import annotations
import argparse, os, random, math
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
import yaml
import pandas as pd

ITEM_MAINS = ["Biryani","Burger","Pizza","Pasta","Thali","Noodles","Fried Rice","Wrap","Sandwich","Curry Meal"]
ITEM_SIDES = ["Fries","Raita","Garlic Bread","Salad","Dip","Papad","Nachos","Coleslaw"]
ITEM_DRINKS = ["Coke","Pepsi","Lemonade","Iced Tea","Cold Coffee","Masala Chaas","Mango Shake","Water"]
ITEM_DESSERTS = ["Gulab Jamun","Brownie","Ice Cream","Pastry","Kheer","Cheesecake","Mousse"]
RAW_CATEGORY_MAP = {
    "main": ["Main Course","Entree","Biryani","Meals","Pasta","Pizza","Burgers"],
    "side": ["Sides","Accompaniments","Snacks"],
    "drink": ["Beverages","Drinks","Tea/Coffee","Juices/Shakes"],
    "dessert": ["Desserts","Sweets"],
    "addon": ["Add-ons","Extras","Toppings"],
    "combo": ["Combos","Meals","Value Combo","Bucket Deals"],
}
CUISINE_POOL = ["North Indian","South Indian","Chinese","Fast Food","Italian","Beverages","Desserts","Street Food","Mughlai"]

def weighted_choice(rng: random.Random, items: List[Tuple[str, float]]) -> str:
    names = [x for x,_ in items]
    weights = [w for _,w in items]
    return rng.choices(names, weights=weights, k=1)[0]

def expand_distribution(rows):
    return [(r["bucket"], float(r["weight"])) for r in rows]

def maybe_parquet(df: pd.DataFrame, path_base: str):
    csv_path = path_base + ".csv"
    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(path_base + ".parquet", index=False)
    except Exception:
        pass
    return csv_path

def make_restaurants(cfg, rng, dists):
    n = int(cfg["targets"]["restaurants"])
    city_dist = expand_distribution(dists["city_weight"])
    price_dist = expand_distribution(dists["restaurant_price_band_weight"])
    rows=[]
    for i in range(1, n+1):
        city = weighted_choice(rng, city_dist)
        zone = f"zone_{rng.randint(1, max(8, int(18 * (1 if city in ['city_1','city_2'] else 0.6)))):02d}"
        cuisines = rng.sample(CUISINE_POOL, k=rng.randint(1,3))
        price_band = int(weighted_choice(rng, price_dist))
        rows.append({
            "restaurant_id": f"R_{i:06d}",
            "restaurant_name": f"{rng.choice(['Spice','Tandoor','Urban','Royal','Quick','Taste','House'])} {rng.choice(['Hub','Kitchen','Bites','Point','Cafe','Corner'])}",
            "city": city,
            "zone": zone,
            "cuisine_tags": "|".join(cuisines),
            "price_band": price_band,
            "avg_rating": round(max(2.5, min(4.9, rng.gauss(4.0, 0.35))), 1),
            "rating_count_bucket": rng.choice(["0-100","101-500","501-2000","2000+"]),
            "pure_veg_flag": rng.random() < 0.22,
            "chain_flag": rng.random() < 0.15,
            "is_active": True if rng.random() > 0.03 else False,
            "created_at": (datetime(2025,1,1) + timedelta(days=rng.randint(0,365))).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)

def make_menu_items(cfg, rng, restaurants_df, dists):
    target = int(cfg["targets"]["menu_items"])
    desc_missing = float(cfg.get("missingness",{}).get("item_description_missing_rate", 0.35) or 0.35)
    raw_cat_missing = float(cfg.get("missingness",{}).get("raw_category_missing_rate", 0.08) or 0.08)
    rows=[]
    # Allocate items per restaurant using weighted random until target reached
    restaurant_ids = restaurants_df["restaurant_id"].tolist()
    rest_info = restaurants_df.set_index("restaurant_id")[["cuisine_tags","pure_veg_flag"]].to_dict("index")
    item_id = 1
    while len(rows) < target:
        rid = rng.choice(restaurant_ids)
        cat = rng.choices(["main","side","drink","dessert","addon","combo"], weights=[0.38,0.18,0.2,0.12,0.08,0.04], k=1)[0]
        if cat == "main":
            name = rng.choice(ITEM_MAINS)
        elif cat == "side":
            name = rng.choice(ITEM_SIDES)
        elif cat == "drink":
            name = rng.choice(ITEM_DRINKS)
        elif cat == "dessert":
            name = rng.choice(ITEM_DESSERTS)
        elif cat == "addon":
            name = rng.choice(["Extra Cheese","Mayo Dip","Spicy Dip","Cheese Slice","Extra Sauce"])
        else:
            name = rng.choice(["Meal Combo","Family Combo","Value Combo","Bucket Meal"])
        # Noise variants
        if rng.random() < 0.08:
            name = name.replace(" ", "") if rng.random()<0.5 else name + f" {rng.choice(['300ml','Regular','(Veg)'])}"
        price_base = {"main":220,"side":90,"drink":65,"dessert":110,"addon":35,"combo":320}[cat]
        price = max(20, round(rng.gauss(price_base, price_base*0.25) / 5) * 5)
        pure_veg = bool(rest_info[rid]["pure_veg_flag"])
        veg_flag = "veg" if pure_veg or rng.random() < 0.55 else "non_veg"
        if cat == "drink" and rng.random() < 0.08:
            veg_flag = "unknown"
        rows.append({
            "item_id": f"I_{item_id:06d}",
            "restaurant_id": rid,
            "item_name": name,
            "item_description": None if rng.random() < desc_missing else f"Tasty {name.lower()} with house seasoning",
            "raw_category": None if rng.random() < raw_cat_missing else rng.choice(RAW_CATEGORY_MAP[cat]),
            "normalized_category": cat,
            "veg_flag": veg_flag,
            "portion_size": rng.choices(["single","regular","family","unknown"], [0.28,0.55,0.07,0.10])[0],
            "course_type": {"main":"meal","side":"snack","drink":"beverage","dessert":"dessert","addon":"snack","combo":"meal"}[cat],
            "cuisine_tags": rest_info[rid]["cuisine_tags"],
            "price": float(price),
            "availability_prob": round(max(0.7, min(1.0, rng.gauss(0.94, 0.05))), 2),
            "is_bestseller": rng.random() < 0.08,
            "created_at": (datetime(2025,1,1) + timedelta(days=rng.randint(0,365))).strftime("%Y-%m-%d"),
            "is_active": True if rng.random() > 0.04 else False,
        })
        item_id += 1
    return pd.DataFrame(rows)

def make_users(cfg, rng, dists):
    n = int(cfg["targets"]["users"])
    city_dist = expand_distribution(dists["city_weight"])
    seg_dist = expand_distribution(dists["user_segment_weight"])
    veg_pref_dist = expand_distribution(dists["user_veg_pref_bucket_weight"])
    history_dist = expand_distribution(dists["user_history_band_weight"])
    rows=[]
    for i in range(1, n+1):
        city = weighted_choice(rng, city_dist)
        rows.append({
            "user_id": f"U_{i:06d}",
            "home_city": city,
            "home_zone": f"zone_{rng.randint(1,14):02d}",
            "user_segment": weighted_choice(rng, seg_dist),
            "price_sensitivity_score": round(min(1, max(0, rng.random()**0.7)), 3),
            "novelty_preference_score": round(min(1, max(0, rng.random())), 3),
            "veg_pref_bucket": weighted_choice(rng, veg_pref_dist),
            "order_frequency_band": weighted_choice(rng, history_dist),
            "signup_cohort_month": (datetime(2025,1,1) + pd.offsets.MonthBegin(rng.randint(0,11))).strftime("%Y-%m"),
            "is_active": True if rng.random() > 0.05 else False
        })
    return pd.DataFrame(rows)

def target_final_cart_size(rng, dists):
    x = weighted_choice(rng, expand_distribution(dists["final_cart_size_weight"]))
    if x == "6_plus":
        return rng.randint(6,10)
    return int(x)

def make_sessions_and_events(cfg, rng, users_df, restaurants_df, menu_df, dists, session_override=None):
    session_n = int(session_override if session_override is not None else cfg["targets"]["sessions"])
    meal_dist = expand_distribution(dists["meal_slot_weight"])
    day_type_dist = expand_distribution(dists["day_type_weight"])
    outcome_dist = expand_distribution(dists["session_outcome_weight"])
    offer_dist = expand_distribution(dists["offer_context_weight"])
    show_sources = expand_distribution(dists["exposure_candidate_source_weight"])

    users = users_df.to_dict("records")
    restaurants = restaurants_df.to_dict("records")
    rest_by_city = defaultdict(list)
    for r in restaurants:
        rest_by_city[r["city"]].append(r)
    items_by_rest = defaultdict(list)
    menu_records = menu_df.to_dict("records")
    for m in menu_records:
        items_by_rest[m["restaurant_id"]].append(m)

    sessions, events, exposures = [], [], []
    base_date = datetime(2025, 10, 1, 0, 0)
    eid = 1
    xid = 1
    for sidx in range(1, session_n + 1):
        u = rng.choice(users)
        city = u["home_city"] if rng.random() < 0.85 else rng.choice(list(rest_by_city.keys()))
        rest = rng.choice(rest_by_city[city] or restaurants)
        meal_slot = weighted_choice(rng, meal_dist)
        day_type = weighted_choice(rng, day_type_dist)
        offer = weighted_choice(rng, offer_dist)
        outcome = weighted_choice(rng, outcome_dist)
        ts = base_date + timedelta(days=rng.randint(0, int(cfg["time_split"]["horizon_days"]) - 1),
                                   hours=rng.randint(0,23), minutes=rng.randint(0,59))
        current_ts = ts  # NEW: monotonic event clock for this session
        session_id = f"S_{sidx:06d}"
        target_size = target_final_cart_size(rng, dists)
        add_steps = 0
        cart = []
        cart_value = 0.0
        prefix_step = 0
        session_added_ids = set()          # NEW: track add_item item_ids for current session only
        session_exposure_start_idx = len(exposures)  # NEW: slice exposures for current session only

        # Optional view_menu exposure
        pre_event_id = f"E_{eid:06d}"
        events.append({
            "event_id": pre_event_id, "session_id": session_id, "event_ts": current_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": "view_menu", "item_id": None, "qty": None, "add_order_index": None,
            "cart_value_before": 0.0, "cart_value_after": 0.0, "cart_size_before": 0, "cart_size_after": 0,
        }); eid += 1
        current_ts = current_ts + timedelta(minutes=rng.randint(0, 2))
        # pre-exposure
        pool = items_by_rest.get(rest["restaurant_id"], [])
        if pool:
            cand_ids = [m["item_id"] for m in rng.sample(pool, k=min(len(pool), int(cfg["generation"]["top_k_exposures"])))]
            for rank, cid in enumerate(cand_ids, start=1):
                source = weighted_choice(rng, show_sources)
                score = round(max(0.01, min(0.99, 1 - (rank-1)*0.08 + rng.uniform(-0.05,0.05))), 3)
                exposures.append({
                    "exposure_id": f"X_{xid:07d}", "session_id": session_id, "event_id": pre_event_id,
                    "prefix_step": prefix_step, "candidate_rank": rank, "candidate_item_id": cid,
                    "candidate_score": score, "source": source, "was_clicked": False,
                    "was_added_within_n_steps": False, "was_added_in_session": False,
                }); xid += 1

        # add/remove/qty events
        while add_steps < target_size:
            pool = items_by_rest.get(rest["restaurant_id"], [])
            if not pool:
                break
            prefix_step += 1
            # choose item, bias by category complement
            if cart:
                cats = [c["normalized_category"] for c in cart]
                preferred = []
                if "main" in cats:
                    preferred += ["side"]*4 + ["drink"]*4 + ["dessert"]*2
                if "drink" not in cats:
                    preferred += ["drink"]*2
                if "dessert" not in cats and add_steps >= 1:
                    preferred += ["dessert"]*2
                target_cat = rng.choice(preferred) if preferred and rng.random() < 0.65 else None
            else:
                target_cat = rng.choices(["main","combo","snack","drink"], weights=[0.55,0.15,0.2,0.1], k=1)[0]
            candidates = [m for m in pool if target_cat is None or m["normalized_category"] == target_cat or (target_cat=="snack" and m["normalized_category"] in ["side","addon"])]
            if not candidates:
                candidates = pool
            item = rng.choice(candidates)
            qty = 1 if rng.random() < 0.92 else 2

            before_v = cart_value
            before_n = sum(c["qty"] for c in cart)
            cart.append({"item_id": item["item_id"], "qty": qty, "normalized_category": item["normalized_category"], "price": float(item["price"])})
            cart_value += float(item["price"]) * qty
            add_steps += 1
            #event_ts = ts + timedelta(minutes=add_steps * rng.randint(1,3))
            event_ts = current_ts
            add_event_id = f"E_{eid:06d}"
            events.append({
                "event_id": add_event_id, "session_id": session_id, "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "add_item", "item_id": item["item_id"], "qty": qty, "add_order_index": add_steps,
                "cart_value_before": round(before_v,2), "cart_value_after": round(cart_value,2),
                "cart_size_before": before_n, "cart_size_after": before_n + qty,
            }); eid += 1
            current_ts = current_ts + timedelta(minutes=rng.randint(0, 3))

            session_added_ids.add(item["item_id"])  # NEW

            # Exposure after add_item
            pool_ids = [m["item_id"] for m in pool]
            shown = []
            # Some complements
            complements = [m["item_id"] for m in pool if m["normalized_category"] in (["side","drink","dessert"] if item["normalized_category"] in ["main","combo"] else ["main","drink","dessert"])]
            rng.shuffle(complements)
            shown.extend(complements[:4])
            remaining = [x for x in pool_ids if x not in shown]
            rng.shuffle(remaining)
            shown.extend(remaining[:max(0, int(cfg["generation"]["top_k_exposures"]) - len(shown))])
            shown = shown[:int(cfg["generation"]["top_k_exposures"])]
            for rank, cid in enumerate(shown, start=1):
                source = weighted_choice(rng, show_sources)
                score = round(max(0.01, min(0.99, 1 - 0.07*(rank-1) + rng.uniform(-0.06,0.06))), 3)
                exposures.append({
                    "exposure_id": f"X_{xid:07d}", "session_id": session_id, "event_id": add_event_id,
                    "prefix_step": prefix_step, "candidate_rank": rank, "candidate_item_id": cid,
                    "candidate_score": score, "source": source, "was_clicked": False,
                    "was_added_within_n_steps": False, "was_added_in_session": False,
                }); xid += 1
            # occasional qty change (NEW)
            if len(cart) >= 1 and rng.random() < float(cfg["generation"].get("qty_change_event_prob_base", 0.05)):
                qidx = rng.randrange(len(cart))
                qitem = cart[qidx]

                old_qty = int(qitem["qty"])
                # Bias toward +1, but allow -1 when qty > 1
                if old_qty <= 1:
                    new_qty = 2
                else:
                    new_qty = old_qty + (1 if rng.random() < 0.65 else -1)

                new_qty = max(1, min(new_qty, 5))  # clamp
                if new_qty != old_qty:
                    before_v_q = cart_value
                    before_n_q = sum(c["qty"] for c in cart)

                    qitem["qty"] = new_qty
                    delta_qty = new_qty - old_qty
                    cart_value += qitem["price"] * delta_qty

                    event_ts_q = current_ts
                    events.append({
                        "event_id": f"E_{eid:06d}", "session_id": session_id, "event_ts": event_ts_q.strftime("%Y-%m-%d %H:%M:%S"),
                        "event_type": "qty_change", "item_id": qitem["item_id"], "qty": new_qty, "add_order_index": None,
                        "cart_value_before": round(before_v_q,2), "cart_value_after": round(cart_value,2),
                        "cart_size_before": before_n_q, "cart_size_after": before_n_q + delta_qty,
                    }); eid += 1

                    current_ts = current_ts + timedelta(minutes=rng.randint(0, 2))
            # occasional remove
            if len(cart) > 1 and rng.random() < float(cfg["generation"].get("remove_event_prob_base", 0.08)):
                ridx = rng.randrange(len(cart))
                rem = cart.pop(ridx)
                before_v2 = cart_value
                before_n2 = sum(c["qty"] for c in cart) + rem["qty"]
                cart_value -= rem["price"] * rem["qty"]
                #event_ts2 = event_ts + timedelta(minutes=1)
                event_ts2 = current_ts
                events.append({
                    "event_id": f"E_{eid:06d}", "session_id": session_id, "event_ts": event_ts2.strftime("%Y-%m-%d %H:%M:%S"),
                    "event_type": "remove_item", "item_id": rem["item_id"], "qty": rem["qty"], "add_order_index": None,
                    "cart_value_before": round(before_v2,2), "cart_value_after": round(cart_value,2),
                    "cart_size_before": before_n2, "cart_size_after": before_n2 - rem["qty"],
                }); eid += 1
                current_ts = current_ts + timedelta(minutes=rng.randint(0, 2))

        # checkout / abandon terminal event
        #terminal_ts = ts + timedelta(minutes=max(1, add_steps*2 + 1))
        terminal_ts = current_ts + timedelta(minutes=rng.randint(0, 2))
        if outcome == "ordered" and cart:
            before_v = cart_value
            before_n = sum(c["qty"] for c in cart)
            events.append({
                "event_id": f"E_{eid:06d}", "session_id": session_id, "event_ts": terminal_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "checkout", "item_id": None, "qty": None, "add_order_index": None,
                "cart_value_before": round(before_v,2), "cart_value_after": round(before_v,2),
                "cart_size_before": before_n, "cart_size_after": before_n,
            }); eid += 1
            order_total = round(before_v,2)
        else:
            before_v = cart_value
            before_n = sum(c["qty"] for c in cart)
            events.append({
                "event_id": f"E_{eid:06d}", "session_id": session_id, "event_ts": terminal_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "abandon", "item_id": None, "qty": None, "add_order_index": None,
                "cart_value_before": round(before_v,2), "cart_value_after": round(before_v,2),
                "cart_size_before": before_n, "cart_size_after": before_n,
            }); eid += 1
            order_total = None

        sessions.append({
            "session_id": session_id,
            "user_id": u["user_id"],
            "restaurant_id": rest["restaurant_id"],
            "city": rest["city"],
            "zone": rest["zone"],
            "session_start_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "meal_slot": meal_slot,
            "day_type": day_type,
            "offer_context": offer,
            "session_outcome": "ordered" if order_total is not None else "abandoned",
            "order_total": order_total,
        })

        # Update exposure labels based on items actually added in session
        # Update exposure labels based on items actually added in THIS session (FAST)
        for ex in exposures[session_exposure_start_idx:]:
            ex["was_added_in_session"] = ex["candidate_item_id"] in session_added_ids
            ex["was_added_within_n_steps"] = ex["was_added_in_session"]  # TODO: implement step-window exact logic
            ex["was_clicked"] = ex["was_added_in_session"] and (ex["candidate_rank"] <= 5)

    return pd.DataFrame(sessions), pd.DataFrame(events), pd.DataFrame(exposures)


def validate_cart_event_monotonicity(cart_events_df: pd.DataFrame):
    df = cart_events_df.copy()
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    bad_sessions = []

    for sid, g in df.groupby("session_id", sort=False):
        if not g["event_ts"].is_monotonic_increasing:
            bad_sessions.append(sid)

    if bad_sessions:
        raise ValueError(
            f"Non-monotonic event_ts found in {len(bad_sessions)} sessions. Examples: {bad_sessions[:10]}"
        )
    
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="generation_config YAML")
    ap.add_argument("--outdir", default="generated_pilot")
    ap.add_argument("--pilot-sessions", type=int, default=10000, help="override session count for quick pilot")
    ap.add_argument("--sample-rows", type=int, default=500)
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    rng = random.Random(int(cfg["project"]["seed"]))

    dists = cfg["distributions"]
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(os.path.join(args.outdir, "samples"), exist_ok=True)

    # Downscale related counts for pilot if requested
    scale = args.pilot_sessions / max(1, int(cfg["targets"]["sessions"]))
    cfg_local = dict(cfg)
    cfg_local["targets"] = dict(cfg["targets"])
    cfg_local["targets"]["sessions"] = args.pilot_sessions
    cfg_local["targets"]["restaurants"] = max(200, int(cfg["targets"]["restaurants"] * max(scale, 0.3)))
    cfg_local["targets"]["menu_items"] = max(4000, int(cfg["targets"]["menu_items"] * max(scale, 0.3)))
    cfg_local["targets"]["users"] = max(5000, int(cfg["targets"]["users"] * max(scale, 0.35)))

    restaurants = make_restaurants(cfg_local, rng, dists)
    menu_items = make_menu_items(cfg_local, rng, restaurants, dists)
    users = make_users(cfg_local, rng, dists)
    sessions, cart_events, reco_exposures = make_sessions_and_events(cfg_local, rng, users, restaurants, menu_items, dists, session_override=args.pilot_sessions)
    validate_cart_event_monotonicity(cart_events)
    tables = {
        "restaurants": restaurants,
        "menu_items": menu_items,
        "users": users,
        "sessions": sessions,
        "cart_events": cart_events,
        "reco_exposures": reco_exposures,
    }
    for name, df in tables.items():
        base = os.path.join(args.outdir, name)
        maybe_parquet(df, base)
        df.head(args.sample_rows).to_csv(os.path.join(args.outdir, "samples", f"{name}_sample.csv"), index=False)

    # quick realism summary
    summary = {
        "row_counts": {k: int(len(v)) for k,v in tables.items()},
        "cart_event_type_counts": cart_events["event_type"].value_counts(dropna=False).to_dict(),
        "session_outcome_counts": sessions["session_outcome"].value_counts(dropna=False).to_dict(),
        "meal_slot_counts": sessions["meal_slot"].value_counts(dropna=False).to_dict(),
        "city_counts": sessions["city"].value_counts(dropna=False).to_dict(),
    }
    pd.Series(summary["row_counts"]).to_csv(os.path.join(args.outdir, "row_counts_summary.csv"), header=["rows"])
    with open(os.path.join(args.outdir, "quick_summary.txt"), "w", encoding="utf-8") as f:
        for k,v in summary.items():
            f.write(f"{k}: {v}\n")
    print("Generated pilot dataset in", args.outdir)
    for k,v in summary["row_counts"].items():
        print(f"{k}: {v:,}")

if __name__ == "__main__":
    main()
