select
    employee_id,
    name,
    title,
    city,
    address,
    annual_salary,
    try_to_date(trim(replace(hire_date, 'day ', ''))) as hire_date
from {{ source('google_drive', 'hr_joins') }}