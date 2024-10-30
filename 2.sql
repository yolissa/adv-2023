
create foreign table day2 (ipt text)
  server adv2023 options(filename '/tmp/day2.txt', null '');

-- part1: max 12 red cubes, 13 green cubes, and 14 blue cubes

    with sets as (
        select 
            regexp_substr(regexp_substr(ipt, 'Game (\d+):'), '\d+')::int as game,
            unnest(
                string_to_array(
                    regexp_replace(ipt,'Game (\d+):', '')
                    , ';'
                )
            ) as set
        from day2
    )
    -- select * from sets;
    , colors_by_set as (
        select 
            game,
            set,
            row_number() over () as set_num,
            array_to_string(regexp_match(set, '(\d+) blue'), ',')::int as blue,
            array_to_string(regexp_match(set, '(\d+) green'), ',')::int as green,
            array_to_string(regexp_match(set, '(\d+) red'), ',')::int as red
        from sets
    )
    -- select  from colors_by_set;
    , max_by_game as (
        select
            game,
            max(blue) as blue,
            max(green) as green,
            max(red) as red
        from colors_by_set
        group by game
    )
    --select * from max_by_game;
    select
        sum(game)
    from max_by_game
    where blue <= 14 and green <= 13 and red <= 12;

-- part 2: sum of multiplication of max of each colour in each game

    with sets as (
        select 
            regexp_substr(regexp_substr(ipt, 'Game (\d+):'), '\d+')::int as game,
            unnest(
                string_to_array(
                    regexp_replace(ipt,'Game (\d+):', '')
                    , ';'
                )
            ) as set
        from day2
    )
    -- select * from sets;
    , colors_by_set as (
        select 
            game,
            set,
            row_number() over () as set_num,
            array_to_string(regexp_match(set, '(\d+) blue'), ',')::int as blue,
            array_to_string(regexp_match(set, '(\d+) green'), ',')::int as green,
            array_to_string(regexp_match(set, '(\d+) red'), ',')::int as red
        from sets
    )
    -- select  from colors_by_set;
    , max_by_game as (
        select
            game,
            max(blue) as blue,
            max(green) as green,
            max(red) as red
        from colors_by_set
        group by game
    )
    --select * from max_by_game;
    select
        sum(blue * green * red)
    from max_by_game;