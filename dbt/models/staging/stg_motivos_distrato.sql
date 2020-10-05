
with source as (

    select * from staging.xmotivodistrato

),

final as (

    select
        codmotivodistrato,
        dscmotivodistrato
        
    from source

)

select * from final