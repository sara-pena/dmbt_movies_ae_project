with

movie_crew_members as (

    select
        movie_id,
        crew_member_id

    from {{ ref('stg_tmdb__movie_crew_members') }}

),

crew_member_attributes as (

    select
        crew_member_id,
        crew_member_name,
        gender

    from {{ ref('int_crew_member_attributes') }}

),

movies as (

    select
        movie_id,
        release_date

    from {{ ref('stg_tmdb__movies') }}

),

crew_member_dimensions as (

    select
        crew_member_attributes.crew_member_id,
        crew_member_attributes.crew_member_name,
        crew_member_attributes.gender,

        max(movies.release_date) as latest_movie_released_on

    from movie_crew_members
    left join crew_member_attributes
        on movie_crew_members.crew_member_id = crew_member_attributes.crew_member_id
    left join movies on movie_crew_members.movie_id = movies.movie_id
    group by 1, 2, 3

)

select * from crew_member_dimensions