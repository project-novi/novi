with cte as (
    select
        id,
        tags,
        (
            select
                sum((select log(count(*)) from object) - coalesce(log(count), 0))
            from
                unnest($1::text[]) ttag
            join
                tag on ttag = tag.tag
            where
                ttag in (select jsonb_object_keys(tags))
        ) -- ::real / sqrt(array_length($1::text[], 1) * (select count(*) from jsonb_object_keys(tags)))
            as similarity
        from
            object
        where
            tags ?| $1
)
select
    tag as "tag!",
    sum(similarity) as "confidence!"
from
    cte cross join jsonb_object_keys(tags) as tag
where
    not tag like '@%'
    and tag not in (
        select
            unnest($1::text[]))
group by tag
order by "confidence!" desc
limit 10;

