
create foreign table day3 (ipt text)
  server adv2023 options(filename '/tmp/day3.txt', null '');

-- part1: sum of adjacent numbers 

    -- split: 1 row = 1 cell with row_number & col_number
    create table cells as (
        select
            row,
            row_number() over (partition by row) as idx,
            cell
        from (
            select 
                row_number() over () as row,
                regexp_split_to_table(ipt, '') as cell
                from day3
        )
    );

    -- find cells matching the pattern
    create table matching_cells as (
        with number_cells as (
            select * from cells where regexp_count(cell, '(\d)') = 1
        )
        , special_cells as (
            select * from cells where regexp_count(cell, '([^\d.])')::bool
        )
        , close_enough as (
            (
                select main.*
                from number_cells main
                inner join special_cells sec on sec.row = main.row and (sec.idx = main.idx -1 or sec.idx = main.idx +1)
            )
            union
            (
                select main.*
                from number_cells main
                inner join special_cells sec on sec.row = main.row-1 and (sec.idx = main.idx -1 or sec.idx = main.idx +1 or sec.idx = main.idx)
            )
            union
            (
                select main.*
                from number_cells main
                inner join special_cells sec on sec.row = main.row+1 and (sec.idx = main.idx -1 or sec.idx = main.idx +1 or sec.idx = main.idx)
            )
            order by row, idx
        
        )
        select * from close_enough
    );

    -- recursively feed the numbers to the left
    create table matching_cells_left as (
        with recursive full_numbers as (
            (
                select main.*, coalesce(l.cell::text, '') || main.cell::text as number, coalesce(l.idx, main.idx) as first_idx
                from matching_cells main
                left join cells l on l.row = main.row and l.idx = main.idx -1 and regexp_count(l.cell, '(\d)') = 1
                order by row, idx
            )
            union
            (
                select main.row, main.idx, main.cell,  coalesce(l.cell::text, '') || main.number::text as number, coalesce(l.idx, main.first_idx) as first_idx
                from full_numbers main
                left join cells l on l.row = main.row and l.idx = main.first_idx -1 and regexp_count(l.cell, '(\d)') = 1 
                where main.first_idx <> main.idx
            )
        )
        , aggregated as (
            select row, idx, cell, max(number::int) as number, min(first_idx) as first_idx, row_number() over (partition by row, min(first_idx) order by idx desc) as row_num
                from full_numbers 
                group by row, idx, cell 
                order by row, idx
        )
        select * from aggregated where row_num = 1
    );


    -- recursively feed the numbers to the right, and sum.
    with recursive full_numbers as (
        (
            select main.row, main.idx, main.cell, main.first_idx, main.number::text || coalesce(r.cell::text, '') as number, coalesce(r.idx, main.idx) as last_idx
            from matching_cells_left main
            left join cells r on r.row = main.row and r.idx = main.idx +1 and regexp_count(r.cell, '(\d)') = 1
            order by row, idx
        )
        union
        (
            select main.row, main.idx, main.cell, main.first_idx, main.number::text || coalesce(r.cell::text, '') as number, coalesce(r.idx, main.last_idx) as last_idx
            from full_numbers main
            left join cells r on r.row = main.row and r.idx = main.last_idx +1 and regexp_count(r.cell, '(\d)') = 1
            where main.last_idx <> main.idx
        )
    )
    , aggregated as (
        select row, idx, cell,  first_idx, max(number::int) as number, max(last_idx) as last_idx
            from full_numbers 
            group by row, idx, cell, first_idx 
            order by row, idx
    )
    select sum(number) from aggregated;

-- part2: filter by the ones matching a gear (*)


    -- find cells matching the pattern
    drop table matching_cells;
    create table matching_cells as (
        with number_cells as (
            select * from cells where regexp_count(cell, '(\d)') = 1
        )
        , special_cells as (
            select * from cells where regexp_count(cell, '(\*)') = 1
        )
        , close_enough as (
            (
                select main.*, sec.row as gear_row, sec.idx as gear_idx
                from number_cells main
                inner join special_cells sec on sec.row = main.row and (sec.idx = main.idx -1 or sec.idx = main.idx +1)
            )
            union
            (
                select main.*, sec.row as gear_row, sec.idx as gear_idx
                from number_cells main
                inner join special_cells sec on sec.row = main.row-1 and (sec.idx = main.idx -1 or sec.idx = main.idx +1 or sec.idx = main.idx)
            )
            union
            (
                select main.*, sec.row as gear_row, sec.idx as gear_idx
                from number_cells main
                inner join special_cells sec on sec.row = main.row+1 and (sec.idx = main.idx -1 or sec.idx = main.idx +1 or sec.idx = main.idx)
            )
            order by row, idx        
        )
        select * from close_enough
    );


    -- recursively feed the numbers to the left
    drop table matching_cells_left;
    create table matching_cells_left as (
        with recursive full_numbers as (
            (
                select  main.row, main.idx, main.cell, coalesce(l.cell::text, '') || main.cell::text as number, coalesce(l.idx, main.idx) as first_idx
                from matching_cells main
                left join cells l on l.row = main.row and l.idx = main.idx -1 and regexp_count(l.cell, '(\d)') = 1
                order by row, idx
            )
            union
            (
                select main.row, main.idx, main.cell, coalesce(l.cell::text, '') || main.number::text as number, coalesce(l.idx, main.first_idx) as first_idx
                from full_numbers main
                left join cells l on l.row = main.row and l.idx = main.first_idx -1 and regexp_count(l.cell, '(\d)') = 1 
                where main.first_idx <> main.idx
            )
        )
        , aggregated as (
            select row, idx, cell, max(number::int) as number, min(first_idx) as first_idx, row_number() over (partition by row, min(first_idx) order by idx desc) as row_num
                from full_numbers 
                group by row, idx, cell 
                order by row, idx
        )
        select * from aggregated where row_num = 1
    );

    -- recursively feed the numbers to the right, feed set with gear data, filter by count of gears, multiply, and sum.
    with recursive full_numbers as (
        (
            select main.row, main.idx, main.cell, main.first_idx, main.number::text || coalesce(r.cell::text, '') as number, coalesce(r.idx, main.idx) as last_idx
            from matching_cells_left main
            left join cells r on r.row = main.row and r.idx = main.idx +1 and regexp_count(r.cell, '(\d)') = 1
            order by row, idx
        )
        union
        (
            select main.row, main.idx, main.cell, main.first_idx, main.number::text || coalesce(r.cell::text, '') as number, coalesce(r.idx, main.last_idx) as last_idx
            from full_numbers main
            left join cells r on r.row = main.row and r.idx = main.last_idx +1 and regexp_count(r.cell, '(\d)') = 1
            where main.last_idx <> main.idx
        )
    )
    , aggregated as (
        select row, idx, cell,  first_idx, max(number::int) as number, max(last_idx) as last_idx
            from full_numbers 
            group by row, idx, cell, first_idx 
            order by row, idx
    )
    , full_table as (
        select matching_cells.*, first_idx, number, last_idx
        from matching_cells
        inner join aggregated on aggregated.row = matching_cells.row and aggregated.idx = matching_cells.idx
    )
    , filtered as (
        select a.gear_row, a.gear_idx, a.number * b.number as ratio
        from full_table a 
        inner join full_table b on (a.gear_row, a.gear_idx) = (b.gear_row, b.gear_idx) and (a.row, a.first_idx) <> (b.row, b.first_idx)
        where (a.gear_row, a.gear_idx) in (
            select gear_row, gear_idx from full_table group by gear_row, gear_idx having count(*) = 2
        )
        group by a.gear_row, a.gear_idx, a.number * b.number
    )
    select sum(ratio) from filtered;