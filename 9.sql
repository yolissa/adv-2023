create foreign table day9 (ipt text)
  server adv2023 options(filename '/tmp/day9.txt', null '');

-- select * from day9;


with recursive withrows as (
  select ipt, row_number() over () as rownum from day9
)
, asrows as (
    select rownum, string_to_table(ipt, ' ')::numeric as history from withrows where rownum = 1
)
, ordered as (
  -- select * from asrows
  select rownum, history, row_number () over (partition by rownum) as rank from asrows
)
, computing as (
    (
      select rownum, history, 0 as loop, rank, count(*) over() as cnt, count(*) filter(where history = 0) as zeros 
      from ordered
      group by rownum, rank, history, loop
      order by rank asc
    )
    union 
    (
      with curr as (
        select * from computing
        where cnt <> zeros and loop < 20 
      )
      select rownum
        , (lead(history, 1) over (partition by rownum order by rank asc) - history) as history
        , loop + 1 as loop
        , rank
        , count(*) over() as cnt
        , count(*) filter(where history = 0) as zeros
        from curr
        group by rownum, rank, history, loop
        order by rank asc
      )
)
select * from computing ;

-- mieux select les lignes avec 0 et ça va le faire