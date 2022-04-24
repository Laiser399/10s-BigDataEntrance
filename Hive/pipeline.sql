create database semenov;



insert overwrite directory '/user/semenov/weird_locations' row format delimited fields terminated by '\t' escaped by '\\' stored as textfile
select
    users.location,
    count(*)
from users
group by
    users.location
order by
    count(*) desc;



create external table semenov.location_mappings (
    weird_location string,
    country        string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = "\t", "quoteChar" = "\"" ) stored as textfile location '/user/semenov/tables/location_mappings';



insert overwrite directory '/user/semenov/tables/users_mapped' row format delimited fields terminated by '\t' escaped by '\\' stored as textfile
select
    id,
    country
from
    default.users u
    inner join semenov.location_mappings m
               on u.location = m.weird_location;

create external table semenov.users_mapped (
    id      int,
    country string
) row format delimited fields terminated by '\t' stored as textfile location '/user/semenov/tables/users_mapped';



-- 17 053 425 - total users
-- 12 999 198 - users with NULL location
-- 3 888 705
with
    a as (
        select location, count(*) users_count from default.users u group by location
    )
select
    location,
    users_count,
    country
from
    a
    left join semenov.location_mappings m
              on a.location = m.weird_location
order by
    users_count desc;



-- 54 741 618 - before map
-- 31 372 938 - after map
insert overwrite directory '/user/semenov/tables/posts_mapped' row format delimited fields terminated by '\t' escaped by '\\' stored as textfile
select
    p.id,
    p.posttypeid,
    p.creationdate,
    u.country
from
    default.posts p
    inner join semenov.users_mapped u
               on p.owneruserid = u.id;

create external table semenov.posts_mapped (
    id            int,
    post_type_id  int,
    creation_date timestamp,
    country       string
) row format delimited fields terminated by '\t' stored as textfile location '/user/semenov/tables/posts_mapped';



-- 83 160 604 - before map
-- 50 144 764 - after map
insert overwrite directory '/user/semenov/tables/comments_mapped' row format delimited fields terminated by '\t' escaped by '\\' stored as textfile
select
    c.id,
    c.creationdate,
    u.country
from
    default.comments c
    inner join semenov.users_mapped u
               on c.userid = u.id;

create external table semenov.comments_mapped (
    id            int,
    creation_date timestamp,
    country       string
) row format delimited fields terminated by '\t' stored as textfile location '/user/semenov/tables/comments_mapped';



-- Всего 195 стран


-- 2008 3q
-- 2021 4q
-- Всего 54 квартала
select
    year(creation_date)    creation_year,
    quarter(creation_date) creation_quarter
from posts_mapped
group by
    year(creation_date), quarter(creation_date)
order by
    creation_year, creation_quarter;


-- Далеко не у всех стран есть 54 квартала
with
    a as (
        select
            country,
            year(creation_date)    creation_year,
            quarter(creation_date) creation_quarter
        from posts_mapped
        group by country, year(creation_date), quarter(creation_date)
    )
select
    country,
    count(*)
from a
group by
    country;



create external table semenov.quarters (
    year    int,
    quarter int
) row format delimited fields terminated by '\t' stored as textfile location '/user/semenov/tables/quarters';



insert overwrite directory '/user/semenov/tables/countries' row format delimited fields terminated by '\t' escaped by '\\' stored as textfile
select distinct
    country
from semenov.location_mappings;

create external table semenov.countries (
    country string
) row format delimited fields terminated by '\t' stored as textfile location '/user/semenov/tables/countries';



with
    startup as (
        select
            country,
            year,
            quarter,
            p.post_type_id
        from
            countries
            cross join quarters
            cross join (
                select
                    p.post_type_id
                from
                    (
                        select 0
                    ) t lateral view explode(array(1, 2)) p as post_type_id
            ) p
    ),
    aggregations as (
        select
            country,
            year(creation_date)    c_year,
            quarter(creation_date) c_quarter,
            post_type_id,
            count(*)               post_count
        from posts_mapped
        group by country, year(creation_date), quarter(creation_date), post_type_id
    )
select
    s.country,
    s.year,
    s.quarter,
    s.post_type_id,
    coalesce(post_count, 0) post_count
from
    startup s
    left join aggregations a
              on s.country = a.country and s.year = a.c_year and s.quarter = a.c_quarter and
                 s.post_type_id = a.post_type_id;


