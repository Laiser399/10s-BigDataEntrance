create
database semenov;



insert
overwrite directory '/user/semenov/weird_locations'
row format delimited
fields terminated by '\t' escaped by '\\'
stored as textfile
select users.location, count(*)
from users
group by users.location
order by count(*) desc;



create
external table semenov.location_mappings(
    weird_location string,
    country string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/semenov/location_mappings';



insert
overwrite directory '/user/semenov/users_mapped'
row format delimited
fields terminated by '\t' escaped by '\\'
stored as textfile
select id, country
from users u
         inner join semenov.location_mappings m
                    on u.location = m.weird_location;

create
external table semenov.users_mapped(
    id int,
    country string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/semenov/users_mapped';



-- 17 053 425 - total users
-- 12 999 198 - users with NULL location
-- 3 888 705
with a as (
    select location, count(*) users_count
    from users u
    group by location
)
select location, users_count, country
from a
         left join semenov.location_mappings m
                   on a.location = m.weird_location
order by users_count desc;



-- 54 741 618 - before map
-- 31 372 689 - after map
insert
overwrite directory '/user/semenov/posts_mapped'
row format delimited
fields terminated by '\t' escaped by '\\'
stored as textfile
select p.id, p.posttypeid, p.creationdate, u.country
from default.posts p
         inner join semenov.users_mapped u
                    on p.owneruserid = u.id;

create
external table semenov.posts_mapped(
    id int,
    post_type_id int,
    creation_date timestamp,
    country string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/semenov/posts_mapped';



-- 83 160 604 - before map
-- 50 144 490 - after map
insert
overwrite directory '/user/semenov/comments_mapped'
row format delimited
fields terminated by '\t' escaped by '\\'
stored as textfile
select c.id, c.creationdate, u.country
from default.comments c
         inner join semenov.users_mapped u
                    on c.userid = u.id;

create
external table semenov.comments_mapped(
    id int,
    creation_date timestamp,
    country string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/semenov/comments_mapped';

