drop table races;
create table races as (
    select * from (values (51, 222), (92, 2031), (68, 1126), (90, 1225)) as tble (t, d)
);
create aggregate mul(bigint) ( SFUNC = int8mul, STYPE=bigint );

-- part 1

    with cases as (
        select
            t
            , d as goal
            , generate_series(1, t-1) as pressed
        from races
    )
    , results as (
        select 
            t
            , goal
            , pressed
            , (t-pressed) * pressed as result
        from cases
    )
    -- select * from results;
    , winners as (
        select
            t
            , goal
            , count(*) as cnt
        from results
        where result > goal
        group by t, goal
    )
    select * from winners;

-- part 2
create table races2 as (
    select * from (values (51926890, 222203111261225)) as tble (t, d)
);
select * from races2;

with computed as (
    select 
        *
        , ceil((t-(sqrt(pow(t, 2) - 4 * d)))/2) as min_x
    from races2
)
select *
, t - 2*min_x +1 as cnt
from computed;

