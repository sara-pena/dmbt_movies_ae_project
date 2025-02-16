with

movie_genres as (

    select
        movie_id,
        genre_id

    from {{ ref('stg_tmdb__movie_genres') }}

),

movies as (

    select
        movie_id,
        release_date,
        movie_status,
        popularity,
        budget,
        revenue

    from {{ ref('stg_tmdb__movies') }}

),

movie_metrics as (

    select
        movie_id,
        roi

    from {{ ref('int_movie_metrics') }}

),

genre_metrics as (

    select
        genre_id,

        count(
            case
                when movies.movie_status = 'Released'
                    then movie_id
            end
        ) as n_movies_released,

        count(
            case
                when movies.movie_status = 'Released'
                    and movies.release_date >= (current_date::date - interval 5 year)::date
                    then movie_id
            end
        ) as n_movies_released_in_past_5_years,

        avg(movies.popularity)as average_popularity,

        avg(movies.budget) as average_budget,

        avg(movies.revenue) as average_revenue,

        avg(movie_metrics.roi) as average_roi

    from movie_genres
    left join movies using (movie_id)
    left join movie_metrics using(movie_id)
    group by 1

),

genre_ranking_by_popularity as (

    select
        genre_id,
        n_movies_released,
        n_movies_released_in_past_5_years,
        average_popularity,
        average_budget,
        average_revenue,
        average_roi,

        rank() over(order by average_popularity desc) as popularity_ranking

    from genre_metrics

)

select * from genre_ranking_by_popularity