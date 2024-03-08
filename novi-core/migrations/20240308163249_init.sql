
create table object(
    id uuid primary key default gen_random_uuid(),
    tags jsonb not null,

    created timestamptz not null,
    updated timestamptz not null,
    creator uuid
);

create index object_tags_idx on object using gin(tags);

create index on object(updated);
create index on object(created);

-- indexes
create index on object((tags->'@user.name'->>'v'));
create index on object((tags->'@tag'->>'v'));

create index on object(creator);

-- access control
create function calculate_access_perms(tags jsonb) returns text[] as $$
  select coalesce(array_agg(substring(key from length('@access.view:') + 1)), '{}')
  from jsonb_each(tags)
  where key like '@access.view:%';
$$ language sql immutable;

alter table object
add column access_perms text[] generated always as (calculate_access_perms(tags)) stored;

create index object_access_perms_idx on object using gin(access_perms);

-- image embeddings
create extension if not exists vector;
create table image_embedding(
    id uuid primary key references object(id) on delete cascade,

    cksum bytea not null,
    embedding vector(768) not null,

    model_cksum bytea not null
);

-- tags
create table tag(
    tag text primary key,
    count int not null,
    search_vector tsvector not null
);

create index on tag using gin(search_vector);
create index on tag (tag text_pattern_ops);
create index on tag (count);

drop table if exists tag_idf;
drop procedure if exists calc_idf();
