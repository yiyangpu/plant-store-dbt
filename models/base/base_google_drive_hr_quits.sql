SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    _FIVETRAN_SYNCED,
    CAST(EMPLOYEE_ID AS STRING) AS EMPLOYEE_ID,
    QUIT_DATE
FROM {{ source('google_drive', 'hr_quits') }}