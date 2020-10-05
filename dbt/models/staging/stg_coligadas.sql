
with source as (

    select * from staging.gcoligada

),

final as (

    select
        codcoligada,
        nome,
        nomefantasia,
        cidade,
        estado,
        cep
        
    from source

)

select * from final 