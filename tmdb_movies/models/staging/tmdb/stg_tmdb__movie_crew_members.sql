with

credits as (

    select
        movie_id,
        crew_array

    from {{ ref('base_tmdb__credits') }}

),

unnested_crew_members as (

    select
        movie_id,
        unnest("crew_array"->'$[*]') as unnested_crew

    from credits

),

extract_fields as (

    select
        movie_id,
        json_extract_string(unnested_crew, '$.id')::int as crew_member_id,
        json_extract_string(unnested_crew, '$.name') as crew_member_name,
        json_extract_string(unnested_crew, '$.gender')::int as gender,
        json_extract_string(unnested_crew, '$.department') as department,
        json_extract_string(unnested_crew, '$.job') as job,
        json_extract_string(unnested_crew, '$.credit_id') as credit_id

    from unnested_crew_members

)

select * from extract_fields