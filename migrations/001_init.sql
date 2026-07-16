DO $$
BEGIN
    CREATE TYPE order_status AS ENUM ('Received', 'Preparing', 'Complete', 'Cancelled');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    customer_id VARCHAR(128) NOT NULL,
    restaurant_id VARCHAR(128) NOT NULL,
    items JSONB NOT NULL CHECK (jsonb_typeof(items) = 'array'),
    status order_status NOT NULL,
    status_event_at TIMESTAMPTZ NOT NULL,
    status_event_id UUID NOT NULL,
    items_event_at TIMESTAMPTZ NOT NULL,
    items_event_id UUID NOT NULL,
    last_event_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS orders_status_last_event_at_idx ON orders (status, last_event_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS orders_last_event_at_idx ON orders (last_event_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pending_order_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS pending_order_events_order_id_idx ON pending_order_events (order_id, occurred_at, event_id);

CREATE TABLE IF NOT EXISTS rejected_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    message_key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    locked_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS outbox_events_unpublished_idx ON outbox_events (created_at) WHERE published_at IS NULL;
