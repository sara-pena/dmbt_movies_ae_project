with

movie_actors as (

    select * from {{ ref('int_movie_actors_deduplicated') }}

),

movie_genres as (

    select
        movie_id,
        genre_id,
        movie_genre_id

    from {{ ref('stg_tmdb__movie_genres') }}

),

movie_genre_metrics as (

    select
        movie_genre_id,
        movie_id,
        genre_id,
        release_date,
        weighted_vote_score,
        roi,
        popularity,
        weighted_score_ranking_within_genre,
        popularity_ranking_within_genre

    from {{ ref('int_movie_genre_metrics') }}

),

actor_genre_scores as (

    select
        movie_actors.actor_id,
        movie_actors.movie_id,

        movie_genres.genre_id,

        movie_genre_metrics.release_date,
        movie_genre_metrics.weighted_vote_score,
        movie_genre_metrics.roi,
        movie_genre_metrics.popularity,
        movie_genre_metrics.weighted_score_ranking_within_genre,
        movie_genre_metrics.popularity_ranking_within_genre

    from movie_actors
    left join movie_genres
        on movie_actors.movie_id = movie_genres.movie_id
    left join movie_genre_metrics 
        on movie_genres.movie_genre_id = movie_genre_metrics.movie_genre_id

),

actor_genre_aggregation as (

    select
        actor_id,
        genre_id,

        count(movie_id) as n_movies_in_genre,

        count(
            case
                when weighted_score_ranking_within_genre <= 25
                    then movie_id
            end
        ) as n_movies_in_top_25_rated_by_genre,

        count(
            case
                when popularity_ranking_within_genre <= 25
                    then movie_id
            end
        ) as n_movies_in_top_25_popular_by_genre,

        max(release_date) as latest_release_date_in_genre

    from actor_genre_scores
    group by 1, 2

),

get_most_popular_movie_by_actor_in_genre as (

    select
        actor_id,
        genre_id,
        movie_id,

        rank() over(
            partition by actor_id, genre_id
            order by popularity desc, weighted_vote_score desc
        ) as _ranking_popularity_by_actor_genre

    from actor_genre_scores

),

consolidate_metrics_by_actor_genre as (

    select
        actor_genre_aggregation.actor_id,
        actor_genre_aggregation.genre_id,
        actor_genre_aggregation.n_movies_in_genre,
        actor_genre_aggregation.n_movies_in_top_25_rated_by_genre,
        actor_genre_aggregation.n_movies_in_top_25_popular_by_genre,
        actor_genre_aggregation.latest_release_date_in_genre,

        get_most_popular_movie_by_actor_in_genre.movie_id as most_popular_movie_id,

        {{ dbt_utils.generate_surrogate_key([
            'actor_id',
            'genre_id'
        ]) }} as actor_genre_id

    from actor_genre_aggregation
    left join get_most_popular_movie_by_actor_in_genre using (actor_id, genre_id)
    where _ranking_popularity_by_actor_genre = 1

)

select * from consolidate_metrics_by_actor_genre