-- DataClaw integration seed
-- 20 tables across 4 schemas (core, marketing, events, derived), >200K rows total
-- Intentional real-world mess included; see comments tagged WART:

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marketing;
CREATE SCHEMA IF NOT EXISTS events;
CREATE SCHEMA IF NOT EXISTS derived;

-- =====================================================================
-- core schema (8 tables) — operational source of truth
-- =====================================================================

CREATE TABLE core.customers (
    id            BIGSERIAL PRIMARY KEY,
    email         TEXT,                   -- WART: nullable; ~3% of rows have NULL email (oauth-only signups)
    full_name     TEXT NOT NULL,
    company       TEXT,
    plan          TEXT NOT NULL DEFAULT 'free',
    country_code  CHAR(2),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ
);
COMMENT ON TABLE core.customers IS 'Canonical customer record. One row per unique signup. Soft-deleted via deleted_at.';
COMMENT ON COLUMN core.customers.plan IS 'Plan slug. Possible values: free, starter, pro, enterprise. Updated by subscription_lifecycle DAG.';

CREATE TABLE core.products (
    id          BIGSERIAL PRIMARY KEY,
    sku         TEXT NOT NULL,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    price_cents INTEGER NOT NULL,         -- WART: cents here, but customer_360.total_revenue is dollars
    active      BOOLEAN NOT NULL DEFAULT true
);
COMMENT ON TABLE core.products IS 'Product catalog. Active=false hides from new orders but retains historical references.';

CREATE TABLE core.discount_codes (
    id          BIGSERIAL PRIMARY KEY,
    code        TEXT UNIQUE NOT NULL,
    pct_off     INTEGER NOT NULL,
    starts_at   TIMESTAMPTZ NOT NULL,
    expires_at  TIMESTAMPTZ
);

CREATE TABLE core.orders (
    id            BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT NOT NULL REFERENCES core.customers(id),
    status        TEXT NOT NULL,         -- WART: enum-as-text; values include 'stuck_in_3ds' which isn't documented anywhere
    total_cents   INTEGER NOT NULL,
    currency      CHAR(3) NOT NULL DEFAULT 'USD',
    discount_id   BIGINT REFERENCES core.discount_codes(id),
    placed_at     TIMESTAMPTZ NOT NULL,
    fulfilled_at  TIMESTAMPTZ,
    refunded_at   TIMESTAMPTZ
);
CREATE INDEX orders_customer_id_idx ON core.orders(customer_id);
CREATE INDEX orders_placed_at_idx ON core.orders(placed_at);
COMMENT ON TABLE core.orders IS 'One row per order. Refreshed nightly by daily_orders_refresh DAG.';
COMMENT ON COLUMN core.orders.status IS 'pending|paid|fulfilled|refunded|canceled|stuck_in_3ds. The last value is undocumented.';

CREATE TABLE core.order_items (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES core.orders(id),
    product_id  BIGINT NOT NULL REFERENCES core.products(id),
    quantity    INTEGER NOT NULL,
    price_cents INTEGER NOT NULL
);
CREATE INDEX order_items_orderid_idx ON core.order_items(order_id);

CREATE TABLE core.payments (
    id              BIGSERIAL PRIMARY KEY,
    order_id        BIGINT NOT NULL REFERENCES core.orders(id),
    stripe_charge_id TEXT,
    amount_cents    INTEGER NOT NULL,
    status          TEXT NOT NULL,
    method          TEXT NOT NULL,
    captured_at     TIMESTAMPTZ NOT NULL
);
COMMENT ON TABLE core.payments IS 'Stripe-backed payment records. Reconciled daily by payments_reconciliation DAG.';

CREATE TABLE core.refunds (
    id            BIGSERIAL PRIMARY KEY,
    payment_id    BIGINT NOT NULL REFERENCES core.payments(id),
    amount_cents  INTEGER NOT NULL,
    reason        TEXT,
    issued_at     TIMESTAMPTZ NOT NULL
);
COMMENT ON TABLE core.refunds IS 'Partial or full refunds. Triggers refund_alerts DAG when 15min count exceeds 3x rolling 7d average.';

CREATE TABLE core.subscriptions (
    id                   BIGSERIAL PRIMARY KEY,
    customer_id          BIGINT NOT NULL REFERENCES core.customers(id),
    plan                 TEXT NOT NULL,
    status               TEXT NOT NULL,    -- trial|active|past_due|paused|canceled
    current_period_start TIMESTAMPTZ,
    current_period_end   TIMESTAMPTZ,
    trial_ends_at        TIMESTAMPTZ,
    canceled_at          TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE core.subscriptions IS 'State machine maintained hourly by subscription_lifecycle DAG.';

-- =====================================================================
-- marketing schema (4 tables)
-- =====================================================================

CREATE TABLE marketing.campaigns (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    platform    TEXT NOT NULL,
    starts_on   DATE NOT NULL,
    ends_on     DATE,
    budget_usd  NUMERIC(12,2)
);

CREATE TABLE marketing.email_sends (
    event_id        TEXT PRIMARY KEY,
    customer_id     BIGINT REFERENCES core.customers(id),
    campaign_id     BIGINT REFERENCES marketing.campaigns(id),
    event_type      TEXT NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL,
    subject         TEXT
);
CREATE INDEX email_sends_customerid_idx ON marketing.email_sends(customer_id);
CREATE INDEX email_sends_sent_at_idx ON marketing.email_sends(sent_at);
COMMENT ON TABLE marketing.email_sends IS 'Raw SendGrid event stream, hourly load. Duplicate event_ids dropped via ON CONFLICT.';

CREATE TABLE marketing.ad_spend_daily (
    date          DATE NOT NULL,
    platform      TEXT NOT NULL,
    campaign_id   BIGINT REFERENCES marketing.campaigns(id),
    impressions   INTEGER NOT NULL,
    clicks        INTEGER NOT NULL,
    spend_usd     NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (date, platform, campaign_id)
);

-- WART: deprecated table left in place
CREATE TABLE marketing.touchpoint_attribution_legacy (
    customer_id  BIGINT,
    touchpoint   TEXT,
    occurred_at  TIMESTAMPTZ,
    weight       NUMERIC
);
COMMENT ON TABLE marketing.touchpoint_attribution_legacy IS 'DEPRECATED 2024-08. Empty. Replaced by derived.attribution_touchpoints.';

-- =====================================================================
-- events schema (3 tables)
-- =====================================================================

CREATE TABLE events.product_events (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT,
    event_type      TEXT NOT NULL,        -- WART: contains both 'signed_up' and 'signup' (typo from old SDK)
    properties      JSONB NOT NULL DEFAULT '{}'::JSONB,
    device_context  JSONB,
    created_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX product_events_userid_idx ON events.product_events(user_id);
CREATE INDEX product_events_event_type_idx ON events.product_events(event_type);

CREATE TABLE events.session_starts (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT,
    session_id    TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    referrer      TEXT
);

CREATE TABLE events.feature_flag_evaluations (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT,
    flag_key      TEXT NOT NULL,
    variant       TEXT NOT NULL,
    evaluated_at  TIMESTAMPTZ NOT NULL
);

-- =====================================================================
-- derived schema (5 tables)
-- =====================================================================

CREATE TABLE derived.customer_360 (
    customer_id      BIGINT PRIMARY KEY REFERENCES core.customers(id),
    total_revenue    NUMERIC(12,2),       -- WART: dollars here, but core.payments.amount_cents is cents
    order_count      INTEGER,
    last_order_at    TIMESTAMPTZ,
    days_since_signup INTEGER,
    segment          TEXT,
    rfm_score        TEXT,
    snapshot_built_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE derived.customer_360 IS 'Wide table; one row per customer. Built weekly Mon 02:00 UTC. WARN: total_revenue in dollars; core.payments uses cents.';

CREATE TABLE derived.cohort_retention (
    cohort_month     DATE NOT NULL,
    weeks_since_signup INTEGER NOT NULL,
    active_customers INTEGER NOT NULL,
    retention_pct    NUMERIC(5,2) NOT NULL,
    PRIMARY KEY (cohort_month, weeks_since_signup)
);

CREATE TABLE derived.attribution_touchpoints (
    order_id      BIGINT NOT NULL,
    touchpoint_id TEXT NOT NULL,
    channel       TEXT NOT NULL,
    weight        NUMERIC(5,4) NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, touchpoint_id)
);

CREATE TABLE derived.payments_reconciled (
    payment_id      BIGINT PRIMARY KEY REFERENCES core.payments(id),
    stripe_match    BOOLEAN NOT NULL,
    refund_offset_cents INTEGER NOT NULL DEFAULT 0,
    net_cents       INTEGER NOT NULL,
    reconciled_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE derived.activation_funnel (
    customer_id              BIGINT PRIMARY KEY REFERENCES core.customers(id),
    signed_up_at             TIMESTAMPTZ,
    verified_email_at        TIMESTAMPTZ,
    created_first_workspace_at TIMESTAMPTZ,
    imported_first_dataset_at TIMESTAMPTZ,
    ran_first_query_at       TIMESTAMPTZ,
    queries_in_first_7d      INTEGER NOT NULL DEFAULT 0,
    activated                BOOLEAN
);

-- =====================================================================
-- DATA: products + discount_codes
-- =====================================================================

INSERT INTO core.products (sku, name, category, price_cents, active)
SELECT
    'SKU-' || lpad(g::text, 4, '0'),
    CASE (g % 5)
      WHEN 0 THEN 'DataClaw Starter'
      WHEN 1 THEN 'DataClaw Pro'
      WHEN 2 THEN 'DataClaw Enterprise'
      WHEN 3 THEN 'DataClaw Add-on Connector'
      ELSE         'DataClaw Add-on Seat'
    END,
    CASE (g % 3) WHEN 0 THEN 'plan' WHEN 1 THEN 'addon' ELSE 'usage' END,
    (1900 + (g * 137 % 30000))::INTEGER,
    g % 11 != 0
FROM generate_series(1, 1000) AS g;

UPDATE core.products SET name = 'DataClaw Pro' WHERE id IN (3, 17, 41);
UPDATE core.products SET name = 'DataClaw Enterprise' WHERE id IN (8, 22);

INSERT INTO core.discount_codes (code, pct_off, starts_at, expires_at)
SELECT
    'PROMO' || lpad(g::text, 3, '0'),
    (5 + (g * 7) % 40)::INTEGER,
    now() - (g || ' days')::INTERVAL,
    CASE WHEN g % 3 = 0 THEN now() + ((g % 90) || ' days')::INTERVAL ELSE NULL END
FROM generate_series(1, 200) AS g;

-- =====================================================================
-- DATA: customers (10000)
-- =====================================================================

INSERT INTO core.customers (email, full_name, company, plan, country_code, created_at, deleted_at)
SELECT
    CASE WHEN g % 33 = 0 THEN NULL ELSE 'user' || g || '@' || (CASE (g % 4) WHEN 0 THEN 'gmail.com' WHEN 1 THEN 'acme.co' WHEN 2 THEN 'startupx.io' ELSE 'bigco.com' END) END,
    'Customer ' || g,
    CASE WHEN g % 5 = 0 THEN NULL ELSE 'Company-' || (g % 1000) END,
    CASE (g * 7 % 10)
      WHEN 0 THEN 'enterprise'
      WHEN 1 THEN 'enterprise'
      WHEN 2 THEN 'pro'
      WHEN 3 THEN 'pro'
      WHEN 4 THEN 'pro'
      WHEN 5 THEN 'starter'
      WHEN 6 THEN 'starter'
      ELSE         'free'
    END,
    (ARRAY['US','GB','DE','FR','IN','CA','AU','BR','JP','MX'])[1 + (g % 10)],
    now() - ((g * 7 % 700) || ' days')::INTERVAL,
    CASE WHEN g % 47 = 0 THEN now() - ((g % 60) || ' days')::INTERVAL ELSE NULL END
FROM generate_series(1, 10000) AS g;

-- =====================================================================
-- DATA: orders + order_items + payments + refunds
-- =====================================================================

INSERT INTO core.orders (customer_id, status, total_cents, currency, discount_id, placed_at, fulfilled_at, refunded_at)
SELECT
    1 + (g * 13 % 10000),
    CASE (g % 100)
      WHEN 0 THEN 'stuck_in_3ds'
      WHEN 1 THEN 'canceled'
      WHEN 2 THEN 'canceled'
      WHEN 3 THEN 'pending'
      WHEN 4 THEN 'pending'
      WHEN 5 THEN 'refunded'
      WHEN 6 THEN 'refunded'
      WHEN 7 THEN 'refunded'
      WHEN 8 THEN 'refunded'
      WHEN 9 THEN 'refunded'
      WHEN 10 THEN 'refunded'
      ELSE 'fulfilled'
    END,
    (2900 + (g * 379 % 95000))::INTEGER,
    (ARRAY['USD','USD','USD','USD','EUR','GBP','CAD'])[1 + (g % 7)],
    CASE WHEN g % 9 = 0 THEN 1 + (g % 50) ELSE NULL END,
    now() - ((g * 4 % 700) || ' hours')::INTERVAL,
    CASE WHEN g % 11 < 8 THEN now() - ((g * 4 % 700) - 1 || ' hours')::INTERVAL ELSE NULL END,
    CASE WHEN g % 100 BETWEEN 5 AND 10 THEN now() - ((g * 4 % 690) || ' hours')::INTERVAL ELSE NULL END
FROM generate_series(1, 50000) AS g;

INSERT INTO core.order_items (order_id, product_id, quantity, price_cents)
SELECT
    o.id,
    1 + ((o.id * pos * 17) % 1000),
    1 + ((o.id * pos) % 4),
    (1900 + ((o.id * pos * 53) % 28000))::INTEGER
FROM core.orders o
CROSS JOIN LATERAL generate_series(1, 1 + (o.id % 4)) AS pos;

INSERT INTO core.payments (order_id, stripe_charge_id, amount_cents, status, method, captured_at)
SELECT
    o.id,
    'ch_' || md5(o.id::text),
    o.total_cents,
    CASE WHEN o.status IN ('canceled','stuck_in_3ds','pending') THEN 'failed' ELSE 'succeeded' END,
    (ARRAY['card','card','card','card','ach','wire','paypal'])[1 + (o.id % 7)],
    o.placed_at + interval '1 minute'
FROM core.orders o
WHERE o.status NOT IN ('pending');

INSERT INTO core.refunds (payment_id, amount_cents, reason, issued_at)
SELECT
    p.id,
    (p.amount_cents * (CASE WHEN o.id % 3 = 0 THEN 100 ELSE 50 END) / 100)::INTEGER,
    (ARRAY['customer_request','duplicate','fraud','product_defect','other'])[1 + (p.id % 5)],
    o.refunded_at
FROM core.payments p
JOIN core.orders o ON o.id = p.order_id
WHERE o.refunded_at IS NOT NULL;

-- Scenario 6 fixture: realistic finance incident for Northstar Retail Group.
-- Northstar is a synthetic enterprise customer used across Postgres, Notion,
-- and Airflow demo fixtures.
WITH northstar AS (
    INSERT INTO core.customers (email, full_name, company, plan, country_code, created_at)
    VALUES (
        'priya.shah@northstar-retail.example',
        'Priya Shah',
        'Northstar Retail Group',
        'enterprise',
        'US',
        now() - interval '180 days'
    )
    RETURNING id
),
northstar_orders AS (
    INSERT INTO core.orders (customer_id, status, total_cents, currency, placed_at, fulfilled_at, refunded_at)
    SELECT northstar.id, data.status, data.total_cents, 'USD', data.placed_at, data.fulfilled_at, data.refunded_at
    FROM northstar
    CROSS JOIN (
        VALUES
            ('fulfilled', 154900, now() - interval '12 hours', now() - interval '11 hours', NULL::timestamptz),
            ('stuck_in_3ds', 154900, now() - interval '1 day', NULL::timestamptz, NULL::timestamptz),
            ('refunded', 489900, now() - interval '7 days', now() - interval '6 days 23 hours', now() - interval '6 days 22 hours'),
            ('fulfilled', 249900, now() - interval '30 days', now() - interval '29 days 23 hours', NULL::timestamptz),
            ('fulfilled', 99900, now() - interval '75 days', now() - interval '74 days 23 hours', NULL::timestamptz)
    ) AS data(status, total_cents, placed_at, fulfilled_at, refunded_at)
    RETURNING id, status, total_cents, placed_at, refunded_at
),
northstar_primary_payments AS (
    INSERT INTO core.payments (order_id, stripe_charge_id, amount_cents, status, method, captured_at)
    SELECT
        id,
        'ch_northstar_' || id::text,
        total_cents,
        CASE WHEN status = 'stuck_in_3ds' THEN 'failed' ELSE 'succeeded' END,
        'card',
        placed_at + interval '1 minute'
    FROM northstar_orders
    RETURNING id, order_id, amount_cents
),
northstar_duplicate_payment AS (
    INSERT INTO core.payments (order_id, stripe_charge_id, amount_cents, status, method, captured_at)
    SELECT
        id,
        'ch_northstar_retry_duplicate_' || id::text,
        total_cents,
        'succeeded',
        'card',
        placed_at + interval '2 minutes'
    FROM northstar_orders
    WHERE status = 'fulfilled'
    ORDER BY placed_at DESC
    LIMIT 1
    RETURNING id, order_id, amount_cents
),
northstar_payments AS (
    SELECT * FROM northstar_primary_payments
    UNION ALL
    SELECT * FROM northstar_duplicate_payment
)
INSERT INTO core.refunds (payment_id, amount_cents, reason, issued_at)
SELECT p.id, p.amount_cents, 'duplicate', o.refunded_at
FROM northstar_payments p
JOIN northstar_orders o ON o.id = p.order_id
WHERE o.status = 'refunded';

-- =====================================================================
-- DATA: subscriptions
-- =====================================================================

INSERT INTO core.subscriptions (customer_id, plan, status, current_period_start, current_period_end, trial_ends_at, canceled_at)
SELECT
    1 + (g * 11 % 10000),
    (ARRAY['starter','pro','enterprise'])[1 + (g % 3)],
    CASE (g % 20)
      WHEN 0 THEN 'trial'
      WHEN 1 THEN 'past_due'
      WHEN 2 THEN 'past_due'
      WHEN 3 THEN 'paused'
      WHEN 4 THEN 'canceled'
      WHEN 5 THEN 'canceled'
      ELSE         'active'
    END,
    now() - ((g % 30) || ' days')::INTERVAL,
    now() + ((30 - g % 30) || ' days')::INTERVAL,
    CASE WHEN g % 20 = 0 THEN now() + ((14 - g % 14) || ' days')::INTERVAL ELSE NULL END,
    CASE WHEN g % 20 BETWEEN 4 AND 5 THEN now() - ((g % 60) || ' days')::INTERVAL ELSE NULL END
FROM generate_series(1, 7000) AS g;

-- =====================================================================
-- DATA: marketing
-- =====================================================================

INSERT INTO marketing.campaigns (name, platform, starts_on, ends_on, budget_usd)
SELECT
    'Campaign-' || g,
    (ARRAY['google_ads','meta','linkedin','tiktok','email'])[1 + (g % 5)],
    (now() - ((g * 5 % 300) || ' days')::INTERVAL)::DATE,
    CASE WHEN g % 4 = 0 THEN (now() + ((g % 60) || ' days')::INTERVAL)::DATE ELSE NULL END,
    (1000 + (g * 1379 % 50000))::NUMERIC(12,2)
FROM generate_series(1, 200) AS g;

INSERT INTO marketing.email_sends (event_id, customer_id, campaign_id, event_type, sent_at, subject)
SELECT
    'evt_' || md5(g::text),
    1 + (g * 7 % 10000),
    1 + (g % 200),
    (ARRAY['send','send','send','send','open','open','open','click','click','bounce','unsubscribe'])[1 + (g % 11)],
    now() - ((g * 3 % 720) || ' hours')::INTERVAL,
    CASE (g % 6)
      WHEN 0 THEN 'Your weekly report is ready'
      WHEN 1 THEN 'New connector — Salesforce'
      WHEN 2 THEN 'Welcome to DataClaw'
      WHEN 3 THEN 'Your trial expires in 7 days'
      WHEN 4 THEN 'Pricing update'
      ELSE         'See what is new in DataClaw'
    END
FROM generate_series(1, 50000) AS g;

INSERT INTO marketing.ad_spend_daily (date, platform, campaign_id, impressions, clicks, spend_usd)
SELECT
    (now() - (d || ' days')::INTERVAL)::DATE,
    p.platform,
    p.id,
    (1000 + (d * 137 + p.id * 53) % 50000)::INTEGER,
    (50 + (d * 17 + p.id * 7) % 1500)::INTEGER,
    (50 + (d * 37 + p.id * 13) % 2500)::NUMERIC(10,2)
FROM generate_series(0, 60) AS d
CROSS JOIN (SELECT id, platform FROM marketing.campaigns WHERE platform != 'email' LIMIT 100) p
ON CONFLICT (date, platform, campaign_id) DO NOTHING;

-- =====================================================================
-- DATA: events
-- =====================================================================

INSERT INTO events.product_events (user_id, event_type, properties, device_context, created_at)
SELECT
    1 + (g * 7 % 10000),
    CASE (g % 20)
      WHEN 0 THEN 'signup'
      WHEN 1 THEN 'signed_up'
      WHEN 2 THEN 'signed_up'
      WHEN 3 THEN 'verified_email'
      WHEN 4 THEN 'created_first_workspace'
      WHEN 5 THEN 'imported_first_dataset'
      WHEN 6 THEN 'ran_first_query'
      WHEN 7 THEN 'checkout_started'
      WHEN 8 THEN 'checkout_completed'
      WHEN 9 THEN 'dashboard_viewed'
      WHEN 10 THEN 'report_generated'
      WHEN 11 THEN 'connector_added'
      WHEN 12 THEN 'team_member_invited'
      WHEN 13 THEN 'agent_run_completed'
      WHEN 14 THEN 'agent_run_completed'
      WHEN 15 THEN 'page_viewed'
      WHEN 16 THEN 'page_viewed'
      WHEN 17 THEN 'page_viewed'
      WHEN 18 THEN 'feature_flag_evaluated'
      ELSE         'session_started'
    END,
    jsonb_build_object('source','web','version', '3.' || (g % 10)),
    CASE WHEN g % 10 < 7 THEN jsonb_build_object('os', (ARRAY['mac','windows','linux','ios','android'])[1 + (g % 5)], 'browser','chrome', 'version','125.0') ELSE NULL END,
    now() - ((g * 5 % 720) || ' minutes')::INTERVAL
FROM generate_series(1, 100000) AS g;

INSERT INTO events.session_starts (user_id, session_id, started_at, referrer)
SELECT
    1 + (g * 11 % 10000),
    'sess_' || md5(g::text),
    now() - ((g * 7 % 1440) || ' minutes')::INTERVAL,
    (ARRAY['google.com','direct','facebook.com','linkedin.com','docs.dataclaw.com',NULL,NULL,NULL])[1 + (g % 8)]
FROM generate_series(1, 30000) AS g;

INSERT INTO events.feature_flag_evaluations (user_id, flag_key, variant, evaluated_at)
SELECT
    1 + (g * 13 % 10000),
    (ARRAY['new_query_editor','agent_v2','knowledge_graph_beta','sql_assistant_v3','dark_mode_default'])[1 + (g % 5)],
    (ARRAY['control','treatment'])[1 + (g % 2)],
    now() - ((g * 3 % 720) || ' minutes')::INTERVAL
FROM generate_series(1, 15000) AS g;

-- =====================================================================
-- DATA: derived
-- =====================================================================

INSERT INTO derived.customer_360 (customer_id, total_revenue, order_count, last_order_at, days_since_signup, segment, rfm_score, snapshot_built_at)
SELECT
    c.id,
    coalesce(sum(o.total_cents)::NUMERIC / 100.0, 0)::NUMERIC(12,2),
    count(o.id)::INTEGER,
    max(o.placed_at),
    extract(day from now() - c.created_at)::INTEGER,
    CASE
      WHEN sum(o.total_cents) > 500000 THEN 'whale'
      WHEN sum(o.total_cents) > 100000 THEN 'core'
      WHEN sum(o.total_cents) > 0 THEN 'casual'
      ELSE 'dormant'
    END,
    'R' || (1 + (c.id % 5)) || 'F' || (1 + (c.id % 5)) || 'M' || (1 + (c.id % 5)),
    now() - interval '14 days'
FROM core.customers c
LEFT JOIN core.orders o ON o.customer_id = c.id
GROUP BY c.id;

INSERT INTO derived.cohort_retention (cohort_month, weeks_since_signup, active_customers, retention_pct)
SELECT
    (date_trunc('month', now() - (m || ' months')::INTERVAL))::DATE,
    w,
    (50 + (m * 13 + w * 7) % 200)::INTEGER,
    GREATEST(5, 100 - w * 6 - m)::NUMERIC(5,2)
FROM generate_series(0, 11) AS m, generate_series(0, 15) AS w;

INSERT INTO derived.attribution_touchpoints (order_id, touchpoint_id, channel, weight, occurred_at)
SELECT
    o.id,
    'tp_' || md5(o.id::text || pos::text),
    (ARRAY['email','google_ads','meta','linkedin','organic'])[1 + ((o.id + pos) % 5)],
    (CASE pos WHEN 1 THEN 0.4 WHEN 2 THEN 0.2 ELSE 0.4 END)::NUMERIC(5,4),
    o.placed_at - (pos * 6 || ' hours')::INTERVAL
FROM core.orders o
CROSS JOIN LATERAL generate_series(1, LEAST(3, 1 + (o.id % 3))) AS pos
WHERE o.status NOT IN ('canceled','stuck_in_3ds','pending')
LIMIT 2500;

INSERT INTO derived.payments_reconciled (payment_id, stripe_match, refund_offset_cents, net_cents, reconciled_at)
SELECT
    p.id,
    p.id % 200 != 0,
    coalesce((SELECT sum(amount_cents) FROM core.refunds r WHERE r.payment_id = p.id), 0)::INTEGER,
    p.amount_cents - coalesce((SELECT sum(amount_cents) FROM core.refunds r WHERE r.payment_id = p.id), 0),
    p.captured_at + interval '1 day'
FROM core.payments p;

INSERT INTO derived.activation_funnel (customer_id, signed_up_at, verified_email_at, created_first_workspace_at, imported_first_dataset_at, ran_first_query_at, queries_in_first_7d, activated)
SELECT
    c.id,
    c.created_at,
    CASE WHEN c.id % 10 < 9 THEN c.created_at + interval '5 minutes' ELSE NULL END,
    CASE WHEN c.id % 10 < 7 THEN c.created_at + interval '12 minutes' ELSE NULL END,
    CASE WHEN c.id % 10 < 5 THEN c.created_at + interval '2 hours' ELSE NULL END,
    CASE WHEN c.id % 10 < 4 THEN c.created_at + interval '4 hours' ELSE NULL END,
    (c.id * 3 % 12)::INTEGER,
    (c.id * 3 % 12) >= 3
FROM core.customers c;

CREATE OR REPLACE VIEW core.active_customers_v AS
  SELECT * FROM core.customers WHERE deleted_at IS NULL;

ANALYZE;
