with

movie_genres as (

    select * from {{ ref('stg_tmdb__movie_genres') }}

)

select
    movie_genre_id,
    movie_id,
    genre_id

from movie_genres