# Zomathon Tier A Generator README

This repo contains the starter generator for the Tier A cart dataset pilot. The current generator is a rule-based synthetic data builder that uses a seeded random number generator, weighted distributions, and a few hardcoded business rules to produce realistic-looking tables.

## What this folder is for

The goal is to generate a consistent, schema-aligned pilot dataset for experimentation, QA, and spreadsheet review before the full Tier A pipeline is finalized.

The starter generator produces these tables:

- restaurants
- menu_items
- users
- sessions
- cart_events
- reco_exposures

It also creates sample CSVs for quick inspection and summary files for sanity checks.

## Files in this folder

### `starter_generator_tierA.py`

This is the main generator script. It is the only file in this folder that actually creates data at runtime.

What it does:

- reads the YAML config
- seeds the random number generator
- creates restaurants, menu items, users, sessions, cart events, and recommendation exposures
- writes CSV output for every table
- tries to write Parquet output if `pyarrow` is available
- writes small sample CSVs for each table
- writes row-count and quick-summary files

How it works at a high level:

1. Load `generation_config_tierA.yaml`.
2. Create a seeded `random.Random` instance.
3. Scale the configured target counts down for a pilot run if `--pilot-sessions` is smaller than the full target.
4. Generate the base dimension tables first: restaurants, menu items, users.
5. Generate sessions, cart events, and recommendation exposures using the earlier tables as context.
6. Validate that cart event timestamps are monotonic within each session.
7. Write outputs to the chosen output directory.

### `generation_config_tierA.yaml`

This is the main control file for the starter generator. It defines:

- the seed
- target row counts
- time horizon settings
- weighted distributions
- missingness rates
- output options
- enum lists used by the generator and downstream QA

This file is the main place where ranges and shapes are defined.

### `schema_tierA.json`

This is a machine-readable schema document for the intended tables and columns.

It is not consumed by the starter generator script directly, but it describes:

- required vs optional columns
- expected types
- example values
- validation notes
- intended meanings of columns

Think of it as the contract for the dataset.

### `prompt_bundle_tierA.json`

This is also not used directly by the starter generator. It contains the later-stage LLM and QA prompts for the broader pipeline, including:

- item normalization prompts
- pairing/tag extraction prompts
- DeepEval judge prompts
- filtering prompts
- QA sampling logic
- validation rules

It exists for the enrichment and review stages, not for the starter data generation itself.

### `NEXT_PHASE_RUNBOOK.md`

This is the operator guide. It explains how to run the generator, inspect outputs, and move from a pilot run to a full Tier A run.

### Generated output folders such as `tierA_v1/`

These folders contain example outputs produced by the starter generator.

Typical contents:

- `restaurants.csv`
- `menu_items.csv`
- `users.csv`
- `sessions.csv`
- `cart_events.csv`
- `reco_exposures.csv`
- `row_counts_summary.csv`
- `quick_summary.txt`
- `samples/`

If Parquet support is available, matching `.parquet` files are also written for each table.

## What each generated file means

### `restaurants.csv`

One row per restaurant.

Main columns:

- `restaurant_id`
- `restaurant_name`
- `city`
- `zone`
- `cuisine_tags`
- `price_band`
- `avg_rating`
- `rating_count_bucket`
- `pure_veg_flag`
- `chain_flag`
- `is_active`
- `created_at`

How values are made:

- city is sampled from the configured city weights
- zone is derived from a coarse zone bucket
- cuisines are sampled from a fixed cuisine pool
- price band is sampled from restaurant price weights
- rating is sampled from a Gaussian around 4.0 and then clamped to a believable range
- active/churn fields are randomized with small probabilities

### `menu_items.csv`

One row per menu item.

Main columns:

- `item_id`
- `restaurant_id`
- `item_name`
- `item_description`
- `raw_category`
- `normalized_category`
- `veg_flag`
- `portion_size`
- `course_type`
- `cuisine_tags`
- `price`
- `availability_prob`
- `is_bestseller`
- `created_at`
- `is_active`

How values are made:

- item type is chosen from a weighted category mix
- item name is drawn from category-specific name pools
- small spelling/noise variants are sometimes added
- descriptions and raw categories are intentionally missing sometimes
- price is derived from a base category price plus Gaussian noise
- veg flags are biased using the restaurant's veg status plus randomness
- availability is sampled near 0.94 and clamped into a narrow believable range

### `users.csv`

One row per user.

Main columns:

- `user_id`
- `home_city`
- `home_zone`
- `user_segment`
- `price_sensitivity_score`
- `novelty_preference_score`
- `veg_pref_bucket`
- `order_frequency_band`
- `signup_cohort_month`
- `is_active`

How values are made:

- city, segment, veg preference, and order history band are all sampled from configured distributions
- scores are generated from bounded random values in the range 0 to 1
- cohort month is generated from a month offset starting in 2025

### `sessions.csv`

One row per session.

Main columns:

- `session_id`
- `user_id`
- `restaurant_id`
- `city`
- `zone`
- `session_start_ts`
- `meal_slot`
- `day_type`
- `offer_context`
- `session_outcome`
- `order_total`

How values are made:

- each session starts with a user and restaurant choice
- meal slot, day type, offer context, and outcome are sampled from weighted distributions
- order total is only present when the session ends in checkout

### `cart_events.csv`

One row per cart event inside a session.

Event types include:

- `view_menu`
- `add_item`
- `remove_item`
- `qty_change`
- `checkout`
- `abandon`

Main columns:

- `event_id`
- `session_id`
- `event_ts`
- `event_type`
- `item_id`
- `qty`
- `add_order_index`
- `cart_value_before`
- `cart_value_after`
- `cart_size_before`
- `cart_size_after`

How values are made:

- every session begins with `view_menu`
- the cart is then built by repeated `add_item` events until the target cart size is reached
- some sessions also get `qty_change` and `remove_item` events
- the session ends with `checkout` or `abandon`
- timestamps are advanced step by step so they stay monotonic within the session

### `reco_exposures.csv`

One row per recommendation exposure candidate.

Main columns:

- `exposure_id`
- `session_id`
- `event_id`
- `prefix_step`
- `candidate_rank`
- `candidate_item_id`
- `candidate_score`
- `source`
- `was_clicked`
- `was_added_within_n_steps`
- `was_added_in_session`

How values are made:

- exposures are created before and after add events
- candidate items are sampled from the restaurant's menu
- ranks are assigned from 1 to `top_k_exposures`
- scores decay by rank with some random noise
- exposure source is sampled from a weighted source mix
- labels are set after the session finishes by checking whether a candidate item was added in the session

## What output the generator gave

The checked-in example output is `tierA_v1/`.

Its row counts are:

- restaurants: 1500
- menu_items: 25000
- users: 40000
- sessions: 80000
- cart_events: 371150
- reco_exposures: 2711854

The quick summary also shows the main outcome balance and session shape:

- ordered sessions: 62501
- abandoned sessions: 17499
- dinner is the largest meal slot
- city_1 has the highest traffic and city_8 is the long tail

## How seed is defined

The seed is defined in `generation_config_tierA.yaml` under:

```yaml
project:
  seed: 42
```

In `starter_generator_tierA.py`, that seed is passed into:

```python
rng = random.Random(int(cfg["project"]["seed"]))
```

That means the same config and the same pilot size should produce the same synthetic pattern every time, assuming the code does not change.

The seed controls:

- sampled cities
- restaurant names and cuisines
- item names and category mix
- user segments and preferences
- session outcomes and meal slots
- cart event timing and event choices
- exposure candidate selection and scoring noise

## How ranges are defined

Ranges are not learned from real data in this starter generator. They are set by configuration and code rules.

There are two main kinds of range control:

### 1. Configured weighted ranges

The YAML file defines probability weights for buckets such as:

- city_weight
- meal_slot_weight
- session_outcome_weight
- final_cart_size_weight
- user_segment_weight
- restaurant_price_band_weight
- offer_context_weight
- exposure_candidate_source_weight

These weights control how often each bucket appears.

### 2. Hardcoded numerical clamps and base values

The Python script defines numerical behavior such as:

- restaurant rating clamped between 2.5 and 4.9
- item availability clamped between 0.7 and 1.0
- cart item quantities clamped between 1 and 5 after quantity change
- item prices centered around category base prices with Gaussian noise

Examples of range logic:

- restaurant ratings are generated with Gaussian noise around 4.0, then clamped
- item prices use category base values like 220 for mains, 90 for sides, 65 for drinks, and 320 for combos, then random noise is added
- availability probabilities are sampled near 0.94 and clipped so they stay realistic

So the ranges come from explicit design choices, not from model inference.

## How names are defined

Names are also mostly rule-based.

### Restaurant names

Restaurant names are created by combining one prefix and one suffix sampled from small lists, for example:

- Spice Hub
- Tandoor Kitchen
- Urban Bites
- Royal Corner

This is done in code using fixed name pools, not an LLM.

### Menu item names

Menu item names come from category-specific pools:

- mains: Biryani, Burger, Pizza, Pasta, Thali, Noodles, Fried Rice, Wrap, Sandwich, Curry Meal
- sides: Fries, Raita, Garlic Bread, Salad, Dip, Papad, Nachos, Coleslaw
- drinks: Coke, Pepsi, Lemonade, Iced Tea, Cold Coffee, Masala Chaas, Mango Shake, Water
- desserts: Gulab Jamun, Brownie, Ice Cream, Pastry, Kheer, Cheesecake, Mousse
- addons: Extra Cheese, Mayo Dip, Spicy Dip, Cheese Slice, Extra Sauce
- combos: Meal Combo, Family Combo, Value Combo, Bucket Meal

Some names get light noise variants such as:

- removing spaces
- adding a suffix like `300ml`, `Regular`, or `(Veg)`

That makes the catalog feel less uniform.

## Why it does not scream synthetic

The generator uses several realism tricks:

- weighted distributions instead of uniform random values
- skewed city and session distributions with a long tail
- missing values in some fields
- noisy raw categories
- slight spelling and formatting variation in item names
- restaurant-item relationships instead of independent random rows
- session-level behavior with event ordering and cart state
- exposure labels derived from session behavior rather than random labels
- churn flags and active/inactive variation

These choices do not make the data real, but they do make it look like product data rather than a flat random spreadsheet.

## How to run it

Example pilot run:

```bash
python starter_generator_tierA.py --config generation_config_tierA.yaml --outdir pilot_v1 --pilot-sessions 10000
```

Example full Tier A run:

```bash
python starter_generator_tierA.py --config generation_config_tierA.yaml --outdir tierA_full --pilot-sessions 80000
```

The runbook in `NEXT_PHASE_RUNBOOK.md` describes the recommended workflow for review and iteration.

## Important limitation

This starter generator is not LLM-driven. The current outputs are created by seeded random sampling and hand-authored business logic.

The LLM-related pieces in `prompt_bundle_tierA.json` are for the next stage of the pipeline, especially menu enrichment and QA, not for the starter pilot generator itself.

## Practical reading order

If you want to understand the system in order, read:

1. `generation_config_tierA.yaml`
2. `starter_generator_tierA.py`
3. `schema_tierA.json`
4. `prompt_bundle_tierA.json`
5. `NEXT_PHASE_RUNBOOK.md`

That order matches how the pilot is configured, generated, validated, and then extended.
