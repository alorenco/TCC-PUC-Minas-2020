
with fcfo as (

    select * from staging.fcfo

),

xcomprador as (

    select * from staging.xcomprador

),

categorias_clientes as (

    select * from  {{ ref('stg_categorias_clientes') }}

),

clientes_categorias as (

    select
        clientes_clustering.codcfo,
        categorias_clientes.categoria_id,
        clientes_clustering.valorm2
        
    from 
        staging.clientes_clustering

    inner join
        categorias_clientes on 
            clientes_clustering.clusters = categorias_clientes.cluster_id
),

final as (

    select 
        fcfo.codcoligada,
        fcfo.codcfo,
        fcfo.nome,
        fcfo.nomefantasia,
        fcfo.bairro,
        fcfo.cidade,
        fcfo.codetd,
        fcfo.cep,
        fcfo.codmunicipio,
        fcfo.dtnascimento,
        fcfo.grpven,
        COALESCE(clientes_categorias.categoria_id, 0)  AS categoria_id,
        COALESCE(clientes_categorias.valorm2, 0)       AS valor_medio_m2,

        -- calculo da idade
        date_part('year',age(fcfo.dtnascimento)) AS idade     
        
    from fcfo

    left join
        clientes_categorias on
            fcfo.codcfo = clientes_categorias.codcfo

    where 
        exists (select 1 
                from 
                    xcomprador
                where 
                    xcomprador.codcfo = fcfo.codcfo
                )
)

select * from final