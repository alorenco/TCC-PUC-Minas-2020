
with clientes_clustering as (

    select * from staging.clientes_clustering

),

final as (

    select
        row_number() over (order by t01.valor_minimo desc) as categoria_id,
        t01.cluster_id,
        t01.valor_minimo,
        t01.valor_maximo
    from (
        select 
            clusters     as cluster_id,
            min(valorm2) as valor_minimo,
            max(valorm2) as valor_maximo
        from 
            clientes_clustering 
        group by 
            clusters
    ) as t01

)

select * from final