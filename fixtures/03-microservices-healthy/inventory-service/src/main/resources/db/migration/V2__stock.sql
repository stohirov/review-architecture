create table stock_items (
    sku       varchar(64) primary key,
    available int    not null check (available >= 0),
    version   bigint not null
);

create table reservations (
    id        bigserial primary key,
    order_id  uuid        not null,
    sku       varchar(64) not null references stock_items (sku),
    quantity  int         not null,
    status    varchar(20) not null,
    unique (order_id, sku)
);
create index reservations_order_idx on reservations (order_id);
