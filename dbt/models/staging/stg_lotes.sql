
with source as (

    select * from staging.xsubunidade

),

final as (

    select
        cod_pess_empr,
        concat(cod_pess_empr, '|', num_unid) AS quadra_id,
        concat(cod_pess_empr, '|', num_unid, '|', num_sub_unid) AS lote_id,
        right(concat('000', rtrim(num_unid)), 3) AS num_unid,
        right(concat('000', rtrim(num_sub_unid)), 3) AS num_sub_unid,
        descricao,
        area,
        cod_sit_sub_unid,
        dsc_sit_sub_unid,
        cod_prefeitura,
        codcoltabprecosubunid,
        numtabprecosubunid
        
    from source

)

select * from final