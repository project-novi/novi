select
    tag as "tag!",
    sum(1.0 / (select count(*) from jsonb_object_keys(tags))::real) as "confidence!"
from
    object
    cross join jsonb_object_keys(tags) as tag
where
    not tag like '@%'
group by
    tag
order by
    "confidence!" desc
limit 10;
