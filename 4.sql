
create foreign table day4 (ipt text)
  server adv2023 options(filename '/tmp/day4.txt', null '');

create extension intarray;

-- part 1

    with ipt_as_arrays as (
        select
            card,
            ipt,
            array_agg(winning::int) filter (where winning is not null) as winning,
            array_agg(hand::int) as hand
        from (
            select
                card,
                ipt, 
                unnest(regexp_matches(substr(ipt, pos_start + 1, pos_sep - pos_start - 1), '([\d]+)', 'g')) as winning,
                unnest(regexp_matches(substr(ipt, pos_sep + 1), '(\d+)', 'g')) as hand
                
                from (
                    select
                        row_number () over () as card,
                        ipt,
                        position('|' in ipt) as pos_sep,
                        position(':' in ipt) as pos_start
                        from day4
                )
        )
        group by card, ipt 
    )
    , intersection as (
        select card, ipt, winning & hand as int, cardinality(winning & hand) as cnt from ipt_as_arrays
    )

    select 
        sum(
            case
                when cnt = 0
                then 0
                else pow(2, cnt-1)
            end
        )
    from intersection;

-- part 2

create table day4_mat as 
    with ipt_as_arrays as (
        select
            card,
            ipt,
            array_agg(winning::int) filter (where winning is not null) as winning,
            array_agg(hand::int) as hand
        from (
            select
                card,
                ipt, 
                unnest(regexp_matches(substr(ipt, pos_start + 1, pos_sep - pos_start - 1), '([\d]+)', 'g')) as winning,
                unnest(regexp_matches(substr(ipt, pos_sep + 1), '(\d+)', 'g')) as hand
                
                from (
                    select
                        row_number () over () as card,
                        ipt,
                        position('|' in ipt) as pos_sep,
                        position(':' in ipt) as pos_start
                        from day4
                )
        )
        group by card, ipt 
    )
    , intersection as (
        select card, cardinality(winning & hand) as cnt from ipt_as_arrays
    )

    select * from intersection;


with recursive base as (
    (
        select card from day4_mat m
        order by card desc
    )
    union all
    (
        select generate_series(curr.card+1, curr.card + m.cnt)
        from base curr
        inner join day4_mat m on m.card = curr.card
    )
)
select count(*) from base;

