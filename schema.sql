CREATE TABLE IF NOT EXISTS "queue" (
	id BIGSERIAL PRIMARY KEY,
	queue VARCHAR,
	state VARCHAR,
	deliveries INT NOT NULL DEFAULT 0,
	leased_until TIMESTAMP WITHOUT TIME ZONE,
	content BYTEA
);
CREATE INDEX IF NOT EXISTS "queue_pop" ON "queue" (queue, state);
CREATE INDEX IF NOT EXISTS "queue_vacuum" ON "queue" (queue, state, deliveries, leased_until);
