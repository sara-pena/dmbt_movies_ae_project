with int_crew_members as (

    select * from {{ ref('int_crew_members') }}

)

select
    crew_member_id,
    crew_member_name,
    gender,
    latest_movie_released_on

from int_crew_members