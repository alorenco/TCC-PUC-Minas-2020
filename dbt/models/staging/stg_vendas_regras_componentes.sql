
with xregracomponentevenda as (

    select * from staging.xregracomponentevenda

),

final as (

    select
        xregracomponentevenda.num_venda,
        xregracomponentevenda.cod_compn,
        xregracomponentevenda.cod_grupo,
        xregracomponentevenda.vr_tx_per,
        xregracomponentevenda.data_base,
        xregracomponentevenda.dat_reaj_ind,
        xregracomponentevenda.cod_grupo_ref

    from xregracomponentevenda

)

select * from final