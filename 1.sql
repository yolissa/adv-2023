create database if not exists adv2023;

--\c adv2023
create extension file_fdw;
create server adv2023 foreign data wrapper file_fdw;

drop foreign table if exists day1;
create foreign table day1 (ipt text)
  server adv2023 options(filename '/tmp/day1.txt', null '');

-- part 1 : select first digit from left, reverse string, and re-select first digit from left

with numbers as (
    select 
        concat(
            regexp_substr(ipt, '(\d)', 1, 1),
            regexp_substr(reverse(ipt), '(\d)', 1, 1)
        ) as str
    from day1
)
--select * from numbers;
select sum(str::int) from numbers;

-- part 2 : first match digit or str from left, reverse string and first match of reversed str or digit from left.
-- then re-reverse result, translate str -> digit, sum everything up.

with digits_as_str as (
    select name from (values ('one'),('two'),('three'),('four'),('five'),('six'),('seven'),('eight'),('nine')) t(name)
)
, regexp as (
    select 
        '(' || string_agg(name, '|') || '|\d' || ')' as pattern_left,
        '(' ||  string_agg(reverse(name), '|') || '|\d' || ')' as pattern_right
    from digits_as_str
)
, matches as (
    select 
        array_to_string(
            regexp_match(ipt, pattern_left)
            , ','
        ) as first,
        reverse(array_to_string(
            regexp_match(reverse(ipt), pattern_right)
            , ','
         )) as last,
        ipt,
        row_number() over () as rownum
    from day1, regexp
)
, translated as (
    select rownum, ipt, first, last,
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
        first
        , '(one)', '1', 'g')
        , '(two)', '2', 'g')
        , '(three)', '3', 'g')
        , '(four)', '4', 'g')
        , '(five)', '5', 'g')
        , '(six)', '6', 'g')
        , '(seven)', '7', 'g')
        , '(eight)', '8', 'g')
        , '(nine)', '9', 'g') as first_t,
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
        last
        , '(one)', '1', 'g')
        , '(two)', '2', 'g')
        , '(three)', '3', 'g')
        , '(four)', '4', 'g')
        , '(five)', '5', 'g')
        , '(six)', '6', 'g')
        , '(seven)', '7', 'g')
        , '(eight)', '8', 'g')
        , '(nine)', '9', 'g') as last_t
    from matches
)
, as_full_number as (
    select *, (first_t || last_t)::int as calibre from translated
)

-- select * from as_full_number;

select sum(calibre) from as_full_number;

