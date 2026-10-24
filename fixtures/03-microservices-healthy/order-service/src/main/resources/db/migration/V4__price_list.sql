create table price_list (
    sku        varchar(64)    primary key,
    unit_price numeric(12, 2) not null check (unit_price > 0),
    active     boolean        not null default true,
    updated_at timestamptz    not null default now()
);
