
with xvendedor as (

    select * from {{ ref('stg_corretores_venda') }}

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
        fcfo.grpven,
        COALESCE(gconsist.descricao, 'Não Definido') AS tipo_corretor, 

        -- calculo da idade
        date_part('year',age(fcfo.dtnascimento)) AS idade
                
    from staging.fcfo

    inner join staging.gconsist
        on fcfo.grpven = gconsist.codcliente 
       and gconsist.codcoligada = 0
       and gconsist.codtabela = 'GrpVen'
       and gconsist.codinterno in ('1', '2')

    where 
        exists (select 1 
                from 
                    xvendedor
                where 
                    xvendedor.codcfo = fcfo.codcfo
                )
)

select * from final