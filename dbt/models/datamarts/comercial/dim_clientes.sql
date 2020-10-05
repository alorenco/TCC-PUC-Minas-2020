{{
config({
    "post-hook": [
      "{{ index(this, 'cliente_id')}}",
    ],
    })
}}

with clientes as (

    select * from {{ ref('stg_clientes') }}

),

adicionais as (

    select * from {{ ref('stg_clientes_dados_adicionais') }}

),

final as (

    select
        clientes.codcfo            AS cliente_id,
        clientes.nome              AS nome,
        clientes.nomefantasia      AS nome_fantasia,
        clientes.bairro            AS bairro,
        clientes.cidade            AS cidade,
        clientes.codetd            AS estado,
        clientes.cep               AS cep,
        clientes.dtnascimento      AS data_nascimento,
        clientes.idade             AS idade,
        clientes.categoria_id      AS categoria_id,
        clientes.valor_medio_m2    AS valor_medio_m2,

        COALESCE(adicionais.nom_prof, 'Não Definido')    AS profissao,
        COALESCE(adicionais.dsc_est_civ, 'Não Definido') AS estado_civil,
        COALESCE(adicionais.nat_cfo, 'Não Definido')     AS naturalidade,
        COALESCE(adicionais.empr_renda, 0)               AS renda,

        case
            when upper(adicionais.cod_sexo) = 'M' then 'Masculino'
            when upper(adicionais.cod_sexo) = 'F' then 'Feminino'
            else 'Não Definido'
        end AS sexo,

        case
            when upper(adicionais.pg_aluguel) = 'S' then 'Sim'
            when upper(adicionais.pg_aluguel) = 'N' then 'Não'
            else 'ND'
        end AS paga_aluguel

    from clientes

    left join adicionais 
        on clientes.codcoligada = adicionais.codcoligada
       and clientes.codcfo = adicionais.codcfo


)

select * from final