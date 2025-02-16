with

movie_crew_members as (

    select
        movie_id,
        crew_member_id,
        crew_member_name,
        gender

    from {{ ref('stg_tmdb__movie_crew_members') }}

),

movies as (

    select
        movie_id,
        release_date

    from {{ ref('stg_tmdb__movies') }}

),

crew_movies_release_date as (

    select
        movie_crew_members.crew_member_id,
        movie_crew_members.crew_member_name,
        movie_crew_members.gender,

        movies.release_date

    from movie_crew_members
    left join movies using (movie_id)

),

get_latest_crew_member_attributes as (

    select
        crew_member_id,
        crew_member_name,
        gender,

        rank() over(partition by crew_member_id order by release_date desc) _ranking_recency_movie_released

    from crew_movies_release_date
)

select * from get_latest_crew_member_attributes
where _ranking_recency_movie_released = 1