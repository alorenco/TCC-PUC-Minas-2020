
with source as (

    select * from staging.xtabprecosubunidade

),

final as (

    select
        codcoltabpreco,
        numtabpreco,
        concat(codpessempr, '|', numunid, '|', numsubunid) AS lote_id,
        precosubunid

    from source

)

select * from final