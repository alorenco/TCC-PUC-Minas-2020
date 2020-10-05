
with xvendedor as (

    select * from {{ ref('stg_gerentes_venda') }}

),

final as (

    select
        fcfo.codcoligada,
        fcfo.codcfo,
        nome, 
        nomefantasia,
        fcfo.bairro,
        fcfo.cidade,
        fcfo.codetd,
        fcfo.cep,
        fcfo.codmunicipio,
        fcfo.dtnascimento,

        -- calculo da idade
        date_part('year', age(fcfo.dtnascimento)) AS idade
                
    from staging.fcfo

    where 
        fcfo.codcfo != '00008551' -- Diretora de Vendas
        and exists (select 1 
                    from 
                       xvendedor
                    where 
                       xvendedor.codcfo = fcfo.codcfo
                    )
)

select * from final