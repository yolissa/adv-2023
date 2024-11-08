
create foreign table day7 (ipt text)
  server adv2023 options(filename '/tmp/day7.txt', null '');

create table hands as (
    with rows as (
        select 
        row_number() over() as rownum 
        , regexp_split_to_array(ipt, ' ') as meta  
        from day7 
    ) 
    select 
    rownum
    , meta[1] as hand
    , meta[2] as bid
    from rows
);
drop table cards;
create table cards (card text, rank text);
insert into cards values 
    ('2', 'M')
    ,('3', 'L')
    ,('4', 'K')
    ,('5', 'J')
    ,('6', 'I')
    ,('7', 'H')
    ,('8', 'G')
    ,('9', 'F')
    ,('T', 'E')
    ,('J', 'D')
    ,('Q', 'C')
    ,('K', 'B')
    ,('A', 'A');

with types (rank, value) as (
    values (7,'5'), (6,'41'), (5,'32'), (4,'311'), (3,'221'), (2,'2111'), (1,'11111')
)
, card_by_hand as (
    select
        hands.*, a.card, a.card_pos, cards.rank
        from hands
        left join lateral string_to_table(hand, null) with ordinality AS a(card, card_pos) on true
        join cards on cards.card = a.card
    -- where rownum < 10
)
-- select * from card_by_hand order by rownum asc, card_pos;
, hand_with_type as (
    select rownum, hand, bid, string_agg(cnt_by_card::text, ''::text) as type
    from (
        select 
            rownum, hand, bid, card
            , count(*) as cnt_by_card
            from card_by_hand
            group by rownum, hand, bid, card
            order by rownum, cnt_by_card desc
    )
    group by rownum, hand, bid
)
, hand_by_type_rank as (
    select m.rownum, m.hand, m.bid, m.type, types.rank as rank_by_type, string_agg(card_by_hand.rank::text, '.' order by card_by_hand.card_pos) as rank_by_card
    from hand_with_type m
    inner join types on value = type
    inner join card_by_hand on card_by_hand.rownum = m.rownum
    group by m.rownum, m.hand, m.bid, m.type, rank_by_type
    order by rank_by_type asc
)
, sorted as (
    select 
    *
    , row_number() over (order by rank_by_type asc , rank_by_card desc) as final_rank
    from hand_by_type_rank
)
select sum(final_rank * bid::int) from sorted;
         

-- part 2


drop table cards;
create table cards (card text, rank text);
insert into cards values 
    ('J', 'N')
    ,('2', 'M')
    ,('3', 'L')
    ,('4', 'K')
    ,('5', 'J')
    ,('6', 'I')
    ,('7', 'H')
    ,('8', 'G')
    ,('9', 'F')
    ,('T', 'E')
    ,('Q', 'C')
    ,('K', 'B')
    ,('A', 'A');

with types (rank, value) as (
    values (7,'5'), (6,'41'), (5,'32'), (4,'311'), (3,'221'), (2,'2111'), (1,'11111')
)
, card_by_hand as (
    select
        hands.*, a.card, a.card_pos, cards.rank
        from hands
        left join lateral string_to_table(hand, null) with ordinality AS a(card, card_pos) on true
        join cards on cards.card = a.card
    -- where hands.hand = 'JJJJJ'
)
, count_by_card as (
    select * , row_number() over (partition by rownum) as row_by_hand from (
    select 
        rownum, hand, bid, card, rank
        , count(*) filter (where card = 'J') as cnt_by_j
        , count(*) filter (where card <> 'J') as cnt_by_card
        from card_by_hand
        group by rownum, hand, bid, card, rank
        order by rownum, cnt_by_card desc, rank asc
    )
)
, replace_jokers as (
    select a.rownum, a.hand, a.bid, a.card, a.rank
    , a.cnt_by_card + coalesce(j.cnt_by_j,0) as cnt_by_card
    from count_by_card a 
    left join count_by_card j on j.rownum = a.rownum and j.cnt_by_j > 0
    where a.row_by_hand = 1
    union
    select a.rownum, a.hand, a.bid, a.card, a.rank, a.cnt_by_card
    from count_by_card a 
    where a.row_by_hand <> 1
)
-- select * from replace_jokers 
--         order by rownum, cnt_by_card desc, rank asc
-- ;
, hand_with_type as (
    select rownum, hand, bid, string_agg(cnt_by_card::text, ''::text) as type
    from (
        select 
            *
            from replace_jokers
            where cnt_by_card > 0
            order by rownum, cnt_by_card desc, rank asc
    )
    group by rownum, hand, bid
)
, hand_by_type_rank as (
    select m.rownum, m.hand, m.bid, m.type, types.rank as rank_by_type, string_agg(card_by_hand.rank::text, '.' order by card_by_hand.card_pos) as rank_by_card
    from hand_with_type m
    inner join types on value = type
    inner join card_by_hand on card_by_hand.rownum = m.rownum
    group by m.rownum, m.hand, m.bid, m.type, rank_by_type
    order by rank_by_type asc
)

, sorted as (
    select 
    *
    , row_number() over (order by rank_by_type asc , rank_by_card desc) as final_rank
    from hand_by_type_rank
)
select sum(final_rank * bid::int) from sorted;
