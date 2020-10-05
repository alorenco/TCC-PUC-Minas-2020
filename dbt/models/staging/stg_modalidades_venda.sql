
with source as (

    select * from staging.xmodalidadevenda

),

final as (

    select
        cod_mod_venda as modalidade_id,
        dsc_mod_venda
        
    from source

)

select * from final