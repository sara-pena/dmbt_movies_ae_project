with

int_genres as (

    select * from {{ ref('int_genres') }}

)

select
    genre_id,
    genre_name

from int_genres