with

movie_genres as (

    select
        movie_id,
        genre_id,
        movie_genre_id

    from {{ ref('stg_tmdb__movie_genres') }}

),

movie_metrics as (

    select
        movie_id,
        popularity,
        vote_count,
        vote_average,
        weighted_vote_score,
        roi,
        release_date

    from {{ ref('int_movie_metrics') }}

),

rank_movie_within_genre as (

    select
        movie_genres.movie_id,
        movie_genres.genre_id,
        movie_genres.movie_genre_id,
        movie_metrics.popularity,
        movie_metrics.vote_count,
        movie_metrics.vote_average,
        movie_metrics. weighted_vote_score,
        movie_metrics.roi,
        movie_metrics.release_date,

        rank() over(
            partition by genre_id
            order by weighted_vote_score desc, popularity desc, vote_count desc
        ) as weighted_score_ranking_within_genre,

        rank() over(
            partition by genre_id
            order by popularity desc, weighted_vote_score desc, vote_count desc
        ) as popularity_ranking_within_genre

    from movie_genres
    left join movie_metrics using (movie_id)

)

select * from rank_movie_within_genre