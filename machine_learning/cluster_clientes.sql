    select 
        a.codcfo,
        t01.valorm2

    from
        fcfo a (nolock)
        
    inner join (
        select 
            x.codcolcfo,
            x.codcfo,
            round(sum(z.area),2) area_comprada,
            round(sum(x.valor_venda), 2) valor_compra,
            round(sum(x.valor_venda) / sum(z.area), 2) as valorm2,
            datediff(dd, max(x.dat_venda), getdate()) dias_ult_compra
        from
            xvenda x (nolock)
        inner join
            xitemvenda y (nolock) on
                x.num_venda = y.num_venda
        inner join
            xsubunidade z (nolock) on
                y.cod_pess_empr = z.cod_pess_empr and
                y.num_unid = z.num_unid and
                y.num_sub_unid = z.num_sub_unid
        inner join
            xempreendimento e (nolock) on
                x.cod_pess_empr = e.cod_pess_empr
        where
            x.valor_venda > 5000 and
            z.area > 0
        group by 
            x.codcolcfo,
            x.codcfo
            
        ) t01 on
            a.codcoligada = t01.codcolcfo and
            a.codcfo = t01.codcfo
            
    where
        a.codcfo not in (
            '00004711', '00004245', '00005324', '00005491', '00000503', '00002075', '00003077', 
            '00015029', '00006002', '00022574', '00008201', '00013577', '00019459', '00001099',
            '00007969', '00000581', '00000434') and 
        exists (
            select 
                1 
            from
                xvenda x01 (nolock)
            where
                x01.codcolcfo = a.codcoligada and
                x01.codcfo = a.codcfo  
        )
