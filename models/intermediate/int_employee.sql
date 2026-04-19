with hr_joins as (

    select * 
    from {{ ref('base_google_drive_hr_joins') }}

),

hr_quits as (

    select * 
    from {{ ref('base_google_drive_hr_quits') }}

),

final as (

    select
        j.employee_id,
        j.name,
        j.title,
        j.city,
        j.address,
        j.annual_salary,
        j.hire_date,

        q.quit_date,

        case 
            when q.quit_date is null then 'active'
            else 'quit'
        end as employee_status,

        case 
            when q.quit_date is null then false
            else true
        end as is_quit

    from hr_joins j
    left join hr_quits q
        on j.employee_id = q.employee_id

)

select * from final