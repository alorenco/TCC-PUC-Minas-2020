
with xitemvenda as (

    select * from staging.xitemvenda

),

xempreendimento as (

    select * from staging.xempreendimento

),

xunidade as (

    select * from staging.xunidade

),

xsubunidade as (

    select * from staging.xsubunidade

),

final as (

    select
        xitemvenda.num_venda,
        concat(xitemvenda.cod_pess_empr, '|', xitemvenda.num_unid) AS quadra_id,
        concat(xitemvenda.cod_pess_empr, '|', xitemvenda.num_unid, '|', xitemvenda.num_sub_unid) AS lote_id,
        
        xempreendimento.nomefantasia AS empreendimento,
        right(concat('000', rtrim(xunidade.num_unid)), 3) AS quadra,
        right(concat('000', rtrim(xsubunidade.num_sub_unid)), 3) AS lote,

        coalesce(xitemvenda.vr_item, 0)      AS vr_item,
        coalesce(xitemvenda.vr_desc, 0)      AS vr_desc,
        coalesce(xitemvenda.vr_acrescimo, 0) AS vr_acrescimo
        
    from xitemvenda

    inner join xempreendimento
        on xitemvenda.cod_pess_empr = xempreendimento.cod_pess_empr

    inner join xunidade
        on xitemvenda.cod_pess_empr = xunidade.cod_pess_empr
       and xitemvenda.num_unid = xunidade.num_unid

    inner join xsubunidade
        on xitemvenda.cod_pess_empr = xsubunidade.cod_pess_empr
       and xitemvenda.num_unid = xsubunidade.num_unid 
       and xitemvenda.num_sub_unid = xsubunidade.num_sub_unid        
)

select * from final