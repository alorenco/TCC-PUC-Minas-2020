
with source as (

    select * from staging.xempreendimento

),

final as (

    select
        cod_pess_empr,
        codcoligada,
        nome,
        nomefantasia,
        cidade,
        uf,
        cep,
        codmunicipio,

        concat(uf, '|', codmunicipio) AS localizacao_id,

        -- Sigla Resumida do Empreendimento
        case 
          when cod_pess_empr = 1  then 'EMP 01'
          when cod_pess_empr = 2  then 'EMP 02'
          when cod_pess_empr = 3  then 'EMP 03'
          when cod_pess_empr = 4  then 'EMP 04'
          when cod_pess_empr = 5  then 'EMP 05'
          when cod_pess_empr = 6  then 'EMP 06'
          when cod_pess_empr = 7  then 'EMP 07'
          when cod_pess_empr = 8  then 'EMP 08'
          when cod_pess_empr = 9  then 'EMP 09'
          when cod_pess_empr = 10 then 'EMP 10'
          when cod_pess_empr = 11 then 'EMP 11'
          when cod_pess_empr = 12 then 'EMP 12'
          when cod_pess_empr = 13 then 'EMP 13'
          when cod_pess_empr = 14 then 'EMP 14'
          when cod_pess_empr = 15 then 'EMP 15'
          when cod_pess_empr = 16 then 'EMP 16'
          when cod_pess_empr = 18 then 'EMP 18'
          when cod_pess_empr = 19 then 'EMP 19'
          when cod_pess_empr = 20 then 'EMP 20'
          when cod_pess_empr = 22 then 'EMP 22'
          when cod_pess_empr = 23 then 'EMP 23'
          when cod_pess_empr = 25 then 'EMP 25'
          when cod_pess_empr = 26 then 'EMP 26'
          when cod_pess_empr = 27 then 'EMP 27'
          when cod_pess_empr = 28 then 'EMP 28'
          when cod_pess_empr = 29 then 'EMP 29'
          when cod_pess_empr = 30 then 'EMP 30'
          when cod_pess_empr = 31 then 'EMP 31'
          when cod_pess_empr = 32 then 'EMP 32'
          when cod_pess_empr = 33 then 'EMP 33'
          when cod_pess_empr = 34 then 'EMP 34'
          else 'Outros'
        end AS sigla
        
    from source

)

select * from final