with

movies as (

    select * from {{ ref('stg_tmdb__movies') }}

),

movie_metrics as (

    select * from {{ ref('int_movie_metrics') }}

)

select
    movies.movie_id,
    movies.budget,
    movies.popularity,
    movies.vote_average,
    movies.vote_count,
    movies.revenue,

    movie_metrics.weighted_vote_score,
    movie_metrics.roi,
    movie_metrics.is_trendy_movie

from movies
left join movie_metrics using (movie_id)