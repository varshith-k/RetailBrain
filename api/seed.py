import random
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import Json


def seed_demo_data(postgres_url: str) -> None:
    # psycopg2 accepts both postgres:// and postgresql://
    url = postgres_url.replace("postgresql://", "postgres://", 1)
    conn = psycopg2.connect(url)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM minute_event_stats")
    if cur.fetchone()[0] > 100:
        cur.close()
        conn.close()
        return

    print("Seeding demo data...")
    random.seed(42)
    now = datetime.utcnow().replace(second=0, microsecond=0)

    CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports"]
    PRICE_RANGES = {
        "electronics": (75.0, 900.0),
        "fashion": (20.0, 250.0),
        "home": (15.0, 450.0),
        "beauty": (8.0, 120.0),
        "sports": (18.0, 500.0),
    }

    # --- 48 hours of per-minute event stats ---
    minute_rows = []
    sales_rows = []
    for i in range(48 * 60):
        minute = now - timedelta(minutes=i)
        hour = minute.hour

        if 9 <= hour <= 21:
            base = 3.0
        elif 6 <= hour < 9 or 21 < hour <= 23:
            base = 1.5
        else:
            base = 0.5

        roll = random.random()
        pv_base = base * 5 if roll < 0.03 else base
        pur_base = 0.05 if roll < 0.06 else base

        page_views = max(1, int(random.gauss(15 * pv_base, 4)))
        add_to_cart = max(0, int(random.gauss(8 * base, 3)))
        purchases = max(0, int(random.gauss(5 * pur_base, 2)))
        revenue = round(sum(random.uniform(20, 500) for _ in range(purchases)), 2)

        minute_rows.append((minute, page_views, add_to_cart, purchases, revenue))
        if purchases > 0:
            sales_rows.append((minute, revenue, purchases))

    cur.executemany(
        "INSERT INTO minute_event_stats (minute, page_views, add_to_cart, purchases, revenue) VALUES (%s,%s,%s,%s,%s)",
        minute_rows,
    )
    cur.executemany(
        "INSERT INTO sales_stats (minute, total_sales, purchase_count) VALUES (%s,%s,%s)",
        sales_rows,
    )

    # --- Product sales for 50 products ---
    product_rows = []
    for product_id in range(1000, 1050):
        category = random.choice(CATEGORIES)
        low, high = PRICE_RANGES[category]
        purchase_count = random.randint(3, 40)
        total_revenue = round(sum(random.uniform(low, high) for _ in range(purchase_count)), 2)
        last_seen = now - timedelta(minutes=random.randint(1, 120))
        product_rows.append((product_id, category, total_revenue, purchase_count, last_seen))

    cur.executemany(
        "INSERT INTO product_sales (product_id, category, total_revenue, purchase_count, last_purchase_at) VALUES (%s,%s,%s,%s,%s)",
        product_rows,
    )

    # --- 20 anomaly alerts (ensure unique minute+type combinations) ---
    seen_keys = set()
    alert_rows = []
    alert_specs = [
        (
            "purchase_drop", "high",
            lambda d: f"Purchase drop detected: purchases are {d['current_purchases']} vs baseline {d['baseline_purchases']}.",
            lambda: {"current_purchases": random.randint(0, 2), "baseline_purchases": round(random.uniform(20, 50), 2)},
        ),
        (
            "traffic_spike", "high",
            lambda d: f"Traffic spike detected: page views are {d['current_page_views']} vs baseline {d['baseline_page_views']}.",
            lambda: {"current_page_views": random.randint(80, 150), "baseline_page_views": round(random.uniform(10, 20), 2)},
        ),
        (
            "abandonment_spike", "medium",
            lambda d: f"Cart abandonment spike: {d['current_abandonment_rate']:.0%} vs baseline {d['baseline_abandonment_rate']:.0%}.",
            lambda: {
                "current_abandonment_rate": round(random.uniform(0.75, 0.95), 4),
                "baseline_abandonment_rate": round(random.uniform(0.15, 0.35), 4),
            },
        ),
    ]

    attempts = 0
    while len(alert_rows) < 20 and attempts < 200:
        attempts += 1
        alert_type, severity, msg_fn, detail_fn = random.choice(alert_specs)
        minute = (now - timedelta(minutes=random.randint(5, 48 * 60))).replace(second=0, microsecond=0)
        key = (minute, alert_type)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        details = detail_fn()
        alert_rows.append((minute, alert_type, severity, msg_fn(details), Json(details), minute))

    cur.executemany(
        "INSERT INTO anomaly_alerts (minute, alert_type, severity, message, details, created_at) VALUES (%s,%s,%s,%s,%s,%s)",
        alert_rows,
    )

    conn.commit()
    cur.close()
    conn.close()
    print("Demo data seeded.")
