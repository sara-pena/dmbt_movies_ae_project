with

movies as (

    select
        movie_id,
        genres_array,

    from {{ ref('base_tmdb__movies') }}

),

unnest_genres as (

    select
        movie_id,
        unnest("genres_array"->'$[*]') as unnested_genre

    from movies

),

extract_fields as (

    select
        movie_id,
        json_extract_string(unnested_genre, '$.id')::int as genre_id,
        json_extract_string(unnested_genre, '$.name') as genre_name

    from unnest_genres

),

add_surrogate_key as (

    select
        movie_id,
        genre_id,

        coalesce(genre_name, 'Unknown') as genre_name,

        {{ dbt_utils.generate_surrogate_key([
            'movie_id',
            'genre_id'
        ]) }} as movie_genre_id

    from movies
    left join extract_fields using (movie_id)

)

select * from add_surrogate_key