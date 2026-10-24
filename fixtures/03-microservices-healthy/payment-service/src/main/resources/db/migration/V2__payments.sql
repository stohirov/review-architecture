create table payments (
    id            varchar(64) primary key,
    order_id      uuid        not null unique,
    customer_id   varchar(64) not null,
    amount        numeric(12, 2) not null,
    status        varchar(20) not null,
    psp_reference varchar(100),
    created_at    timestamptz not null
);
