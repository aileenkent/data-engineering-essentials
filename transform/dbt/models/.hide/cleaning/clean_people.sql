{{
  config(
    materialized='external', 
    location='output/clean_people.csv', 
    format='csv'
  )
}}

-- Clean people data by removing rows with missing last_name or dob values
-- This model pulls data from S3 and applies data quality filters

SELECT 
    id,
    first_name,
    last_name,
    email,
    ip_address,
    dob
FROM 
    {{ source('s3_people', 'people') }}

WHERE 
    -- Remove rows where last_name is null or empty
    last_name IS NOT NULL 
    AND TRIM(last_name) != ''
    
    -- Remove rows where dob is null or empty
    AND dob IS NOT NULL 
    AND TRIM(CAST(dob AS VARCHAR)) != ''

ORDER BY 
    last_name,
    first_name
