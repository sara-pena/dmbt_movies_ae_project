with

source as (

    select * from {{ source('tmdb_duckdb', 'movies') }}

),

renamed as (

    select
        id as movie_id,
        title, 
        genres as genres_array,
        budget::int as budget,
        original_language,
        release_date,
        popularity,
        vote_average,
        vote_count,
        status as movie_status,
        revenue,
        runtime,
        production_companies as production_companies_array,
        production_countries as production_countries_array

    from source

)

select * from renamed