with

crew_members as (

    select
        movie_id,
        crew_member_id
    
    from {{ ref('stg_tmdb__movie_crew_members') }}

),

deduplicate as (

    select
        movie_id,
        crew_member_id

    from crew_members
    group by 1, 2

)

select * from deduplicate