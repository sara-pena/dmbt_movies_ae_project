with

cast_members as (

    select
        credit_id,
        cast_order

    from {{ ref('stg_tmdb__movie_cast_members') }}

),

movie_scores as (

    select
        credit_id,

        case
            when cast_order between 0 and 1
                then 'Lead'
            when cast_order between 2 and 5
                then 'Main Supporting'
            when cast_order between 6 and 10
                then 'Supporting Cast'
            when cast_order between 11 and 20
                then 'Minor Role'
            else 'Extra'
        end as cast_category

    from cast_members

)

select * from movie_scores