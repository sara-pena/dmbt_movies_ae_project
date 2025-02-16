with

movie_genres as (

    select
        genre_id,
        genre_name

    from {{ ref('stg_tmdb__movie_genres') }}

),

unique_genres as (

    select
        genre_id,
        genre_name

    from movie_genres
    group by 1, 2

)

select * from unique_genres