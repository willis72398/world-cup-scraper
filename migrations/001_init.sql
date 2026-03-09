-- World Cup 2026 Ticket Bot — initial schema
-- Executed automatically by the postgres Docker image on first startup.
-- (Any .sql file in /docker-entrypoint-initdb.d/ runs once on a fresh DB.)

-- ---------------------------------------------------------------------------
-- worldcup_alerts
-- One row per SMS alert sent.  Used to enforce the per-(game, section, source)
-- cooldown and to suppress re-alerts unless the price drops another 10%.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS worldcup_alerts (
    id         BIGSERIAL      PRIMARY KEY,
    game       TEXT           NOT NULL,            -- e.g. "FIFA World Cup 2026 - Match 23"
    game_date  TEXT           NOT NULL,            -- ISO date, e.g. "2026-06-15"
    section    TEXT           NOT NULL,            -- field_level | lower_bowl | upper_deck | any
    source     TEXT           NOT NULL,            -- ticketmaster | seatgeek | stubhub | vividseats | gametime
    price      NUMERIC(10, 2) NOT NULL,            -- lowest price at time of alert
    url        TEXT           NOT NULL,
    alerted_at TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Fast lookup for cooldown checks: find the most recent alert for a
-- (game, section, source) tuple quickly.
CREATE INDEX IF NOT EXISTS worldcup_alerts_lookup
    ON worldcup_alerts (game, section, source, alerted_at DESC);
