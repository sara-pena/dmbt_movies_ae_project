with

movies as (

    select * from {{ ref('base_tmdb__movies') }}

),

selected_fields as (

    select
        movie_id,
        title,
        budget,
        original_language,
        release_date,
        popularity,
        vote_average,
        vote_count,
        movie_status,
        revenue,
        runtime

    from movies

)

select * from selected_fields