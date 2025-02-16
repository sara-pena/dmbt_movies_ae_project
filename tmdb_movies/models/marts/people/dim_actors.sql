with int_actors as (

    select * from {{ ref('int_actors') }}

)

select
    actor_id,
    actor_name,
    gender,
    latest_movie_released_on

from int_actors