{{
    config(
        materialized='incremental',
        unique_key='credit_id'
    )
}}

with crew_members as (

    select * from {{ ref('stg_tmdb__movie_crew_members') }}

)

select
    credit_id,
    movie_id,
    crew_member_id,
    department,
    job

from crew_members