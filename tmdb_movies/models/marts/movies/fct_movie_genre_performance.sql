with

movie_genres as (

    select * from {{ ref('stg_tmdb__movie_genres') }}

),

movie_ranking_within_genre as (

    select * from {{ ref('int_movie_genre_metrics') }}

)

select
    movie_genres.movie_genre_id,
    movie_genres.movie_id,
    movie_genres.genre_id,

    movie_ranking_within_genre.weighted_score_ranking_within_genre,
    movie_ranking_within_genre.popularity_ranking_within_genre

from movie_genres
left join movie_ranking_within_genre using (movie_id, genre_id)