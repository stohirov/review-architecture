create table outbox (
    id            uuid primary key,
    topic         varchar(200) not null,
    aggregate_key varchar(100) not null,
    type          varchar(100) not null,
    payload       text         not null,
    created_at    timestamptz  not null,
    sent_at       timestamptz
);
create index outbox_unsent_idx on outbox (created_at) where sent_at is null;
create index outbox_sent_at_idx on outbox (sent_at);

create table processed_messages (
    message_id   varchar(64) primary key,
    processed_at timestamptz not null
);
create index processed_messages_at_idx on processed_messages (processed_at);

create table shedlock (
    name       varchar(64)  primary key,
    lock_until timestamptz  not null,
    locked_at  timestamptz  not null,
    locked_by  varchar(255) not null
);
