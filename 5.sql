
-- create foreign table day5 (ipt text)
--   server adv2023 options(filename '/tmp/day5.txt', null '');

--   create table seeds as (
--     select unnest(regexp_matches(ipt, '\d+', 'g')) from day5 where ipt like 'seeds: %'
--   );
--   alter table seeds rename unnest to seed;
--   alter table seeds alter column seed type numeric using seed::numeric;

-- part 1

with withnum as (
    select row_number() over () as rownum, ipt from day5
)
, seed_to_soil as (
    select rownum, regexp_split_to_array(ipt, ' ') as meta 
    from withnum 
    where rownum >= 4 and rownum < 11
)
, seed_to_soil_ranged as (
    select
    rownum, meta,
        numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
        numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
        meta[1]::numeric - meta[2]::numeric as diff
    from seed_to_soil
)
, soil_to_fertilizer as (
    select rownum, regexp_split_to_array(ipt, ' ') as meta 
    from withnum 
    where rownum >= 13 and rownum < 59
)
, soil_to_fertilizer_ranged as (
    select
    rownum, meta,
        numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
        numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
        meta[1]::numeric - meta[2]::numeric as diff
    from soil_to_fertilizer
)
, fertilizer_to_water as (
    select rownum, regexp_split_to_array(ipt, ' ') as meta 
    from withnum 
    where rownum >= 61 and rownum < 110
)
, fertilizer_to_water_ranged as (
    select
    rownum, meta,
        numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
        numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
        meta[1]::numeric - meta[2]::numeric as diff
    from fertilizer_to_water
)
, water_to_light as (
    select rownum, regexp_split_to_array(ipt, ' ') as meta 
    from withnum 
    where rownum >= 112 and rownum < 135
)
, water_to_light_ranged as (
    select
    rownum, meta,
        numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
        numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
        meta[1]::numeric - meta[2]::numeric as diff
    from water_to_light
)
, light_to_temp as (
    select rownum, regexp_split_to_array(ipt, ' ') as meta 
    from withnum 
    where rownum >= 137 and rownum < 183
)
, light_to_temp_ranged as (
    select
    rownum, meta,
        numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
        numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
        meta[1]::numeric - meta[2]::numeric as diff
    from light_to_temp
)
, temp_to_humidity as (
    select rownum, regexp_split_to_array(ipt, ' ') as meta 
    from withnum 
    where rownum >= 185 and rownum < 201
)
, temp_to_humidity_ranged as (
    select
    rownum, meta,
        numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
        numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
        meta[1]::numeric - meta[2]::numeric as diff
    from temp_to_humidity
)
, humidity_to_location as (
    select rownum, regexp_split_to_array(ipt, ' ') as meta 
    from withnum 
    where rownum >= 203 and rownum < 227
)
, humidity_to_location_ranged as (
    select
    rownum, meta,
        numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
        numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
        meta[1]::numeric - meta[2]::numeric as diff
    from humidity_to_location
)
, cte_soil as (
    select 
        seed,
        case 
            when seed_to_soil_ranged.source @> seed
            then seed + seed_to_soil_ranged.diff
            else seed
            end as soil

    from seeds
    left join seed_to_soil_ranged on seed_to_soil_ranged.source @> seed
)
, cte_fertilizer as (
    select 
        seed,
        soil,
        case 
            when soil_to_fertilizer_ranged.source @> soil
            then soil + soil_to_fertilizer_ranged.diff
            else soil
            end as fertilizer

    from cte_soil
    left join soil_to_fertilizer_ranged on soil_to_fertilizer_ranged.source @> soil
)
, cte_water as (
    select 
        seed,
        soil,
        fertilizer,
        case 
            when fertilizer_to_water_ranged.source @> fertilizer
            then fertilizer + fertilizer_to_water_ranged.diff
            else fertilizer
            end as water

    from cte_fertilizer
    left join fertilizer_to_water_ranged on fertilizer_to_water_ranged.source @> fertilizer
)
, cte_light as (
    select 
        seed,
        soil,
        fertilizer,
        water,
        case 
            when water_to_light_ranged.source @> water
            then water + water_to_light_ranged.diff
            else water
            end as light

    from cte_water
    left join water_to_light_ranged on water_to_light_ranged.source @> water
)
, cte_temperature as (
    select 
        seed,
        soil,
        fertilizer,
        water,
        light,
        case 
            when light_to_temp_ranged.source @> light
            then light + light_to_temp_ranged.diff
            else light
            end as temperature

    from cte_light
    left join light_to_temp_ranged on light_to_temp_ranged.source @> light
)
, cte_humidity as (
    select 
        seed,
        soil,
        fertilizer,
        water,
        light,
        temperature,
        case 
            when temp_to_humidity_ranged.source @> temperature
            then temperature + temp_to_humidity_ranged.diff
            else temperature
            end as humidity

    from cte_temperature
    left join temp_to_humidity_ranged on temp_to_humidity_ranged.source @> temperature
)
, cte_location as (
    select 
        seed,
        soil,
        fertilizer,
        water,
        light,
        temperature,
        humidity,
        case 
            when humidity_to_location_ranged.source @> humidity
            then humidity + humidity_to_location_ranged.diff
            else humidity
            end as location

    from cte_humidity
    left join humidity_to_location_ranged on humidity_to_location_ranged.source @> humidity
)

select min(location) from cte_location ;





-- part 2: generate seeds
create table seeds_ranged as (
    with seeds_withnum as (
        select row_number() over () as rownum, seed
        from seeds
    )
    -- select * from withnum;
    , series as (
        select 
        m.rownum, m.seed as start, m.seed + l.seed as stop
        from seeds_withnum m
        inner join seeds_withnum l on l.rownum = m.rownum + 1
        where m.rownum % 2 = 1
    )
    , seed_ranged as (
        select rownum as seed_num, numrange(start, stop) as seeds from series
    )
    select * from seed_ranged
);

create table maps as (
 with all_rows as (
    select row_number() over () as rownum, 
    ipt
    from day5
    where regexp_count(ipt, ':') = 0 or ipt is null
)
, empty_rows as (
    select m.* ,
    row_number() over (partition by ipt order by rownum asc) as catnum
    from all_rows m
    order by rownum asc
)
, categorized as (
    select a.rownum
        , a.ipt
        , regexp_split_to_array(a.ipt, ' ') as meta 
        , (select max(b.catnum) from empty_rows b where b.rownum <= a.rownum ) as catnum
    from empty_rows a
    order by a.rownum asc
)
select 
    row_number() over (partition by catnum order by catnum asc) as rownum,
    catnum,
    numrange(meta[1]::numeric, meta[1]::numeric + meta[3]::numeric) as dest,
    numrange(meta[2]::numeric, meta[2]::numeric + meta[3]::numeric) as source,
    meta[1]::numeric - meta[2]::numeric as diff
    from categorized
    where ipt is not null
);


-- compute

with recursive ending as (
    select max(catnum) as max_cat from maps
)
, a as (
    (
        select seed_num as base_row
        , 0 as curr_cat
        , seeds as base_interval
        from seeds_ranged
    )
    union 
    (
        with ipt as (
            select * from a
        )
        , b as (
            select ipt.*
                , ipt.base_interval * maps.source as match
                , numrange(lower(base_interval * maps.source) + diff, upper(base_interval * maps.source) + diff) as dest_match
            from ipt
            inner join maps on catnum = ipt.curr_cat + 1 and maps.source && ipt.base_interval
        )
        , b_agg as (
            select 
                b.base_row
                , b.base_interval
                , b.curr_cat
                , range_agg(match) as matches
                , nummultirange(b.base_interval) - range_agg(match) as dest_leftovers
                , range_agg(dest_match) as dest_matches
            from b
            group by b.base_row, b.base_interval, b.curr_cat
        )
        select
            row_number() over () as base_row
            , curr_cat + 1 as curr_cat
            , base_interval
            from (
                select
                    base_row
                    , curr_cat
                    , unnest(dest_matches) as base_interval
                from b_agg
                union all
                select
                    base_row
                    , curr_cat
                    , unnest(dest_leftovers) as base_interval
                from b_agg
                union all
                select
                    ipt.base_row
                    , ipt.curr_cat
                    , ipt.base_interval
                from ipt
                left join maps on catnum = ipt.curr_cat + 1 and maps.source && ipt.base_interval
                    where maps.rownum is null 
                    and ipt.curr_cat < (select max_cat from ending)
                order by base_row asc
            )
    )
)
select min(lower(base_interval)) from a where curr_cat = (select max_cat from ending);

