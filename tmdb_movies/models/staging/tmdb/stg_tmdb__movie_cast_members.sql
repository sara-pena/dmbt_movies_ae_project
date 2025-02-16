with

credits as (

    select
        movie_id,
        cast_array

    from {{ ref('base_tmdb__credits') }}

),

unnest_cast_members as (

    select
        movie_id,
        unnest("cast_array"->'$[*]') as unnested_cast

    from credits

),

extract_fields as (

    select
        movie_id,
        json_extract_string(unnested_cast, '$.credit_id') as credit_id,
        json_extract_string(unnested_cast, '$.id')::int as actor_id,
        json_extract_string(unnested_cast, '$.name') as actor_name,
        json_extract_string(unnested_cast, '$.gender')::int as gender,
        json_extract_string(unnested_cast, '$.character') as character_name,
        json_extract_string(unnested_cast, '$.order')::int as cast_order

    from unnest_cast_members

)

select * from extract_fields