SELECT
    EMPLOYEE_ID,
    QUIT_DATE

FROM {{ source('google_drive', 'hr_quits') }}