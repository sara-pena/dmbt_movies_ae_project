with

movie_cast_members as (

    select
        movie_id,
        actor_id,
        actor_name,
        gender

    from {{ ref('stg_tmdb__movie_cast_members') }}

),

movies as (

    select
        movie_id,
        release_date

    from {{ ref('stg_tmdb__movies') }}

),

actor_movies_release_date as (

    select
        movie_cast_members.actor_id,
        movie_cast_members.actor_name,
        movie_cast_members.gender,

        movies.release_date

    from movie_cast_members
    left join movies using (movie_id)

),

get_latest_actor_attributes as (

    select
        actor_id,
        actor_name,
        gender,

        rank() over(partition by actor_id order by release_date desc) _ranking_recency_movie_released

    from actor_movies_release_date
)

select * from get_latest_actor_attributes
where _ranking_recency_movie_released = 1