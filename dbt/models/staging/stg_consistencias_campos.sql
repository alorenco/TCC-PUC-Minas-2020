
with source as (

    select * from staging.gconsist

),

final as (

    select
        codcoligada,
        aplicacao,
        codtabela,
        codcliente,
        codinterno,
        descricao     
        
    from source

)

select * from final