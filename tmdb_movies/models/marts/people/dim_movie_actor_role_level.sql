{{
    config(
        materialized='incremental',
        unique_key='credit_id'
    )
}}

with cast_members as (

    select * from {{ ref('stg_tmdb__movie_cast_members') }}

),

cast_category as (

    select * from {{ ref('int_movie_actors__cast_category') }}

)

select
    credit_id,
    movie_id,
    actor_id,
    cast_order,
    character_name,

    cast_category.cast_category

from cast_members
left join cast_category using (credit_id)