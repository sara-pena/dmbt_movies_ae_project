with

movie_cast_members as (

    select
        movie_id,
        actor_id

    from {{ ref('stg_tmdb__movie_cast_members') }}

),

actors_attributes as (

    select
        actor_id,
        actor_name,
        gender

    from {{ ref('int_actors_attributes') }}

),

movies as (

    select
        movie_id,
        release_date

    from {{ ref('stg_tmdb__movies') }}

),

actor_dimensions as (

    select
        actors_attributes.actor_id,
        actors_attributes.actor_name,
        actors_attributes.gender,

        max(movies.release_date) as latest_movie_released_on

    from movie_cast_members
    left join actors_attributes
        on movie_cast_members.actor_id = actors_attributes.actor_id
    left join movies on movie_cast_members.movie_id = movies.movie_id
    group by 1, 2, 3

)

select * from actor_dimensions