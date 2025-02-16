with

movies as (

    select * from {{ ref('stg_tmdb__movies') }}

)

select
    movie_id,
    title,
    original_language,
    release_date,
    movie_status,
    runtime

from movies