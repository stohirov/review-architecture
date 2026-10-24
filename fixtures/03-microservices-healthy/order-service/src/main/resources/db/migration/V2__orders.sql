create table orders (
    id              uuid primary key,
    request_key     varchar(100) not null unique,
    customer_id     varchar(64)  not null,
    total           numeric(12, 2) not null,
    status          varchar(20)  not null,
    cancel_reason   varchar(200),
    payment_id      varchar(64),
    created_at      timestamptz  not null,
    version         bigint       not null
);
create index orders_customer_created_idx on orders (customer_id, created_at desc);

create table order_items (
    id          bigserial primary key,
    order_id    uuid        not null references orders (id),
    sku         varchar(64) not null,
    quantity    int         not null,
    unit_price  numeric(12, 2) not null
);
create index order_items_order_idx on order_items (order_id);
