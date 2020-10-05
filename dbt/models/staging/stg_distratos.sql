
with source as (

    select * from staging.xdistrato

),

final as (

    select
        numvenda,
        databasedistrato,
        coalesce(codtipodistrato, 0)   as codtipodistrato,
        coalesce(codmotivodistrato, 0) as codmotivodistrato
        
    from source

)

select * from final