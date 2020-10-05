
with xvendaparcela as (

    select * from staging.xvendaparcela

),

lancamentos as (

    select * from {{ ref('stg_lancamentos_receber') }}

),

final as (

    select
        xvendaparcela.codcoligada,
        xvendaparcela.numvenda,
        xvendaparcela.codgrupo,
        xvendaparcela.numparc,
        xvendaparcela.codtipoparc,
        xvendaparcela.numnotapromissoria,

        lancamentos.idlan,
        lancamentos.statuslan,
        lancamentos.codtdo,
        lancamentos.datavencimento,
        lancamentos.permuta,
        lancamentos.valor,
        lancamentos.valor_recebido,
        lancamentos.valor_aberto

    from xvendaparcela

    inner join lancamentos
        on xvendaparcela.codcollan = lancamentos.codcoligada
       and xvendaparcela.idlan = lancamentos.idlan 

)

select * from final