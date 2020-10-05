
with clientes_dados_adicionais as (

    select * from staging.xclientepessoafisica

),

final as (

    select
        codcoligada,
        codcfo,
        nom_prof,
        dsc_est_civ,
        cod_sexo,
        nat_cfo,
        pg_aluguel,
        casa_propria,
        empr_renda
        
    from clientes_dados_adicionais
)

select * from final