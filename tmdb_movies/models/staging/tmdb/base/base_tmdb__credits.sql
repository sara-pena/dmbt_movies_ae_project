with

source as (

    select * from {{ source('tmdb_duckdb', 'credits') }}

),

renamed as (

    select
        movie_id,
        "cast" as cast_array,
        crew as crew_array

    from source

)

select * from renamed