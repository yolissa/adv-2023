create foreign table day8 (ipt text)
  server adv2023 options(filename '/tmp/day8.txt', null '');


create table tree as (
    with rows as (
        select 
        row_number() over() as rownum 
        , ipt 
        from day8
    ) 
    , rows_as_array as (
        select 
        regexp_split_to_array(ipt, '[^A-Z]+') as m
        from rows
        where rownum > 2
    )
    select m[1] as pos, m[2] as left, m[3] as right
    from rows_as_array
);
create table instructions as (
    with rows as (
        select 
        row_number() over() as rownum 
        , ipt 
        from day8
    ) 
    , operations_row as (
        select string_to_table(ipt, null) as op
        from rows 
        where rownum = 1 
    ) 
    , operations as (
        select * from operations_row
    )
    select row_number () over () as i, op from operations
);


-- part 1

with recursive actions as (
    select *, 0 ::bigint as step from tree where pos = 'AAA'
    union all
    select * from (
        with prev as (
            select * from actions
        )
        , op as (
            select op as next 
            from instructions, prev
            where i =  case when (prev.step + 1)%307 = 0
                then 307 -- 307 = count(instructions) btw
                else (prev.step + 1)%307
                end
        )
        select tree.*, prev.step + 1 as step
        from tree, op, prev
        where tree.pos <> 'ZZZ' 
        AND tree.pos = (
            case when op.next = 'L' then prev.left 
            else prev.right
            end
        )
    )
)
select max(step) +1 from actions;

 -- part 2



with recursive actions as (
    select *, 0 ::bigint as step from tree where pos like '%A'
    union all
    select * from (
        with prev as (
            select *, 0 ::bigint as step from tree where pos like '%A'
        )
        , op as (
            select op as next 
            from instructions, prev
            where i =  case when (prev.step + 1)%307 = 0
                then 307 -- 307 = count(instructions) btw
                else (prev.step + 1)%307
                end
        )
        , curr as (
            select tree.*, prev.step + 1 as step
            from tree, op, prev
            where tree.pos = (
                case when op.next = 'L' then prev.left 
                else prev.right
                end
            )
            group by tree.pos, tree.left, tree.right, step
        )
        , kpis as (
            select 
                count(*) filter (where pos like '%B') as nb_z
                , count(*) as nb 
            from curr
        )
         select * from curr, kpis ;
    ) 
)
select max(step) +1 from actions;


with recursive actions as (
    select *, 0 ::bigint as step from tree where pos like '%A'
    union all
    select * from (
        with prev as (
            select * from actions
        )
        , op as (
            select op as next 
            from instructions, prev
            where i =  case when (prev.step + 1)%307 = 0
                then 307 -- 307 = count(instructions) btw
                else (prev.step + 1)%307
                end
        )
        , curr as (
            select tree.*, prev.step + 1 as step
            from tree, op, prev
            where tree.pos = (
                case when op.next = 'L' then prev.left 
                else prev.right
                end
            )
           group by tree.pos, tree.left, tree.right, step
        )
        , kpis as (
            select 
                count(*) filter (where pos like '%Z') as nb_z
                , count(*) as nb 
            from curr
        )
        select curr.* from curr, kpis where case when nb_z = nb then false else true end
    ) 
)
select max(step) +1 from actions;


-- part 2 , voir  :( :( :(
https://github.com/tmercieca/advent-of-code-2023/blob/main/day-8/part-2.sql