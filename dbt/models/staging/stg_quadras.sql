
with source as (

    select * from staging.xunidade

),

final as (

    select
        concat(cod_pess_empr, '|', num_unid) AS quadra_id,
        cod_pess_empr,
        right(concat('000', rtrim(num_unid)), 3) AS num_unid,
        dsc_local,
        qtd_sub_unid
        
    from source

)

select * from final