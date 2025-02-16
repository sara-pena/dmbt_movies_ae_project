with

movies as (

    select
        movie_id,
        popularity,
        vote_count,
        vote_average,
        release_date,
        revenue,
        budget

    from {{ ref('stg_tmdb__movies') }}

),

movie_computations as (

select
    *,

    round((vote_count * 0.7) + (vote_average * 0.3), 2) as weighted_vote_score,

    case
        when revenue > 10000 and budget > 1000
            then round(revenue/budget, 2)
    end as roi,

    row_number() over(order by popularity desc) as _popularity_ranking

from movies

),

final_movie_metrics as (

    select
        movie_id,
        popularity,
        vote_count,
        vote_average,
        release_date,
        weighted_vote_score,
        roi,

        case
            when _popularity_ranking <= 100
                then true
            else false
        end as is_trendy_movie

    from movie_computations

)

select * from final_movie_metrics