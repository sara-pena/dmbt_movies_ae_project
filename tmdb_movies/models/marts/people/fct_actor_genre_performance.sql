with actor_genres as (

    select * from {{ ref('int_actor_genre_metrics') }}

)

select
    actor_genre_id,
    actor_id,
    genre_id,
    n_movies_in_genre,
    n_movies_in_top_25_rated_by_genre,
    n_movies_in_top_25_popular_by_genre,
    latest_release_date_in_genre,
    most_popular_movie_id

from actor_genres