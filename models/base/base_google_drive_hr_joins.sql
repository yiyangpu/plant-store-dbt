SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    _FIVETRAN_SYNCED,
    CAST(EMPLOYEE_ID AS STRING) AS EMPLOYEE_ID,
    NAME,
    TITLE,
    CITY,
    ADDRESS,
    ANNUAL_SALARY,
    TRY_TO_DATE(TRIM(REPLACE(HIRE_DATE, 'day ', ''))) as HIRE_DATE
FROM {{ source('google_drive', 'hr_joins') }}