with

cast_members as (

    select
        movie_id,
        actor_id

    from {{ ref('stg_tmdb__movie_cast_members') }}

),

deduplicate as (

    select
        movie_id,
        actor_id

    from cast_members
    group by 1, 2

)

select * from deduplicate