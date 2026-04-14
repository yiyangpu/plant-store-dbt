SELECT
    EMPLOYEE_ID,
    NAME,
    TITLE,
    CITY,
    ADDRESS,
    ANNUAL_SALARY,

    TO_DATE(HIRE_DATE) AS hire_date

FROM {{ source('google_drive', 'hr_joins') }}