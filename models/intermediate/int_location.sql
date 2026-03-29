with geolocation as (
    select *
    from {{ ref('stg_location') }}
),

ranked as (
    select
        g.*,
        row_number() over (
            partition by g.location_id
            order by g.location_id
        ) as rn
    from geolocation g
),

mapped as (
    select
        *,
        CASE state_names
            WHEN 'SP' THEN 'São Paulo'
            WHEN 'RJ' THEN 'Rio de Janeiro'
            WHEN 'ES' THEN 'Espírito Santo'
            WHEN 'MG' THEN 'Minas Gerais'
            WHEN 'BA' THEN 'Bahia'
            WHEN 'SE' THEN 'Sergipe'
            WHEN 'PE' THEN 'Pernambuco'
            WHEN 'RN' THEN 'Rio Grande do Norte'
            WHEN 'AL' THEN 'Alagoas'
            WHEN 'PB' THEN 'Paraíba'
            WHEN 'CE' THEN 'Ceará'
            WHEN 'PI' THEN 'Piauí'
            WHEN 'MA' THEN 'Maranhão'
            WHEN 'PA' THEN 'Pará'
            WHEN 'AP' THEN 'Amapá'
            WHEN 'AM' THEN 'Amazonas'
            WHEN 'RR' THEN 'Roraima'
            WHEN 'AC' THEN 'Acre'
            WHEN 'DF' THEN 'Distrito Federal'
            WHEN 'GO' THEN 'Goiás'
            WHEN 'RO' THEN 'Rondônia'
            WHEN 'TO' THEN 'Tocantins'
            WHEN 'MT' THEN 'Mato Grosso'
            WHEN 'MS' THEN 'Mato Grosso do Sul'
            WHEN 'PR' THEN 'Paraná'
            WHEN 'SC' THEN 'Santa Catarina'
            WHEN 'RS' THEN 'Rio Grande do Sul'
            ELSE 'Undefined'
        END AS state_full_name
    from ranked
    where rn = 1
)

select * from mapped