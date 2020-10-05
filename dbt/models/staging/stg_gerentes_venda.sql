
with xvendedor as (

    select * from staging.xvendedor

),

fcfo as (

    select * from staging.fcfo

),

final as (

    select
        xvendedor.codcoligada,
        xvendedor.num_venda,
        xvendedor.idvendedor,
        fcfo.codcfo,
        fcfo.grpven      
        
    from xvendedor

    inner join fcfo
        on xvendedor.codcolcfo = fcfo.codcoligada
       and xvendedor.codcfo = fcfo.codcfo

    where
        fcfo.grpven = '3'
        
        and xvendedor.idvendedor = (select
                                        max(t01.idvendedor)
                                    from 
                                        staging.xvendedor as t01
                                    inner join
                                        staging.fcfo as t02 on
                                            t01.codcolcfo = t02.codcoligada and
                                            t01.codcfo = t02.codcfo
                                    where
                                        t02.codcfo != '00008551' and -- Diretora de Vendas
                                        t02.grpven = '3' and
                                        t01.codcoligada = xvendedor.codcoligada and
                                        t01.num_venda = xvendedor.num_venda)
     
)

select * from final