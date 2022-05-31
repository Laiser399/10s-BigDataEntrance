create database semenov;


-- Time taken: 14.1 seconds
-- f:off
insert overwrite directory '/user/semenov/weird_locations'
row format delimited
    fields terminated by '\t' escaped by '\\'
stored as textfile
-- f:on
select
    location,
    count(*)
from default.users
group by
    location
order by
    count(*) desc;



create external table semenov.location_mappings (
    weird_location string,
    country        string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = "\t", "quoteChar" = "\"" ) stored as textfile location '/user/semenov/tables/location_mappings';


-- Time taken: 10.224 seconds
-- f:off
insert overwrite directory '/user/semenov/tables/users_mapped'
row format delimited fields
    terminated by '\t' escaped by '\\'
stored as textfile
-- f:on
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
)
-- f:off
row format delimited fields
    terminated by '\t'
stored as textfile
location '/user/semenov/tables/users_mapped';
-- f:on


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
-- Time taken: 20.109 seconds
-- f:off
insert overwrite directory '/user/semenov/tables/posts_mapped'
row format delimited fields
    terminated by '\t' escaped by '\\'
stored as textfile
-- f:on
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
-- Time taken: 18.603 seconds
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
from semenov.posts_mapped
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
        from semenov.posts_mapped
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


-- Time taken: 20.554 seconds
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
                select explode(array(1, 2)) post_type_id
            ) p
    ),
    aggregations as (
        select
            country,
            year(creation_date)    year,
            quarter(creation_date) quarter,
            post_type_id,
            count(*)               post_count
        from posts_mapped
        group by country, year(creation_date), quarter(creation_date), post_type_id
    )
-- @f:off
insert overwrite directory '/user/semenov/tables/posts_aggregated'
row format delimited
    fields terminated by '\t' escaped by '\\'
stored as textfile
-- @f:on
select
    s.country,
    s.year,
    s.quarter,
    s.post_type_id,
    coalesce(post_count, 0) as post_count
from
    startup s
    left join aggregations a
              on s.country = a.country and s.year = a.year and s.quarter = a.quarter and
                 s.post_type_id = a.post_type_id;

create external table semenov.posts_aggregated (
    country      string,
    year         int,
    quarter      int,
    post_type_id int,
    post_count   int
) row format delimited fields terminated by '\t' stored as textfile location '/user/semenov/tables/posts_aggregated';


-- Time taken: 23.89 seconds
with
    startup as (
        select
            country,
            year,
            quarter
        from
            countries
            cross join quarters
    ),
    aggregations as (
        select
            country,
            year(creation_date)    year,
            quarter(creation_date) quarter,
            count(*)               comment_count
        from comments_mapped
        group by country, year(creation_date), quarter(creation_date)
    )
-- @f:off
insert overwrite directory '/user/semenov/tables/comments_aggregated'
    row format delimited
        fields terminated by '\t' escaped by '\\'
    stored as textfile
-- @f:on
select
    s.country,
    s.year,
    s.quarter,
    coalesce(comment_count, 0) as comment_count
from
    startup s
    left join aggregations a
              on s.country = a.country and s.year = a.year and s.quarter = a.quarter;

create external table semenov.comments_aggregated (
    country       string,
    year          int,
    quarter       int,
    comment_count int
)
-- @f:off
row format delimited
    fields terminated by '\t'
stored as textfile
location '/user/semenov/tables/comments_aggregated';
-- @f:on


set hive.merge.tezfiles = true;


-- Time taken: 3.801 seconds
-- @f:off
insert overwrite directory '/user/semenov/tables/questions_aggregated'
row format delimited
    fields terminated by '\t' escaped by '\\'
stored as textfile
-- @f:on
select
    country,
    year,
    quarter,
    post_count as count
from semenov.posts_aggregated
where
    post_type_id = 1
order by
    country, year, quarter;

create external table semenov.questions_aggregated (
    country string,
    year    int,
    quarter int,
    count   int
)
-- @f:off
row format delimited
    fields terminated by '\t'
stored as textfile
location '/user/semenov/tables/questions_aggregated';
-- @f:on


-- Time taken: 3.945 seconds
-- @f:off
insert overwrite directory '/user/semenov/tables/answers_aggregated' row format delimited fields terminated by '\t' escaped by '\\' stored as textfile
-- @f:on
select
    country,
    year,
    quarter,
    post_count as count
from semenov.posts_aggregated
where
    post_type_id = 2
order by
    country, year, quarter;

create external table semenov.answers_aggregated (
    country string,
    year    int,
    quarter int,
    count   int
)
-- @f:off
row format delimited
    fields terminated by '\t'
stored as textfile
location '/user/semenov/tables/answers_aggregated';
-- @f:on
