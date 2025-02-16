with

genre_metrics as (

    select * from {{ ref('int_genre_metrics') }}

)

select
    genre_id,
    n_movies_released,
    n_movies_released_in_past_5_years,
    average_popularity,
    average_budget,
    average_revenue,
    average_roi,
    popularity_ranking

from genre_metrics