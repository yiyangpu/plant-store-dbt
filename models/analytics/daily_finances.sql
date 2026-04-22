WITH bounds AS (

    SELECT
        LEAST(
            (SELECT MIN(DATE(order_time)) FROM {{ ref('int_orders') }}),
            (SELECT MIN(expense_date) FROM {{ ref('int_expenses') }}),
            (SELECT MIN(hire_date) FROM {{ ref('int_employee') }})
        ) AS start_date,
        CURRENT_DATE() AS end_date

),

date_spine AS (

    SELECT
        DATEADD(
            day,
            ROW_NUMBER() OVER (ORDER BY seq4()) - 1,
            b.start_date
        ) AS date
    FROM bounds b,
         TABLE(GENERATOR(ROWCOUNT => 10000))
    QUALIFY date <= b.end_date

),

daily_revenue AS (

    SELECT
        DATE(order_time) AS date,
        SUM(net_revenue) AS order_revenue
    FROM {{ ref('int_orders') }}
    GROUP BY 1

),

daily_salary AS (

    SELECT
        d.date,
        SUM(e.annual_salary / 365.0) AS employee_salary
    FROM date_spine d
    LEFT JOIN {{ ref('int_employee') }} e
        ON e.hire_date <= d.date
       AND (e.quit_date IS NULL OR e.quit_date >= d.date)
    GROUP BY 1

),

daily_expenses AS (

    SELECT
        expense_date AS date,
        hr_cost,
        warehouse_cost,
        tech_tool_cost,
        other_cost,
        total_expense AS total_expenses
    FROM {{ ref('int_expenses') }}

),

final AS (

    SELECT
        d.date,

        COALESCE(r.order_revenue, 0) AS order_revenue,
        COALESCE(s.employee_salary, 0) AS employee_salary,

        COALESCE(e.hr_cost, 0) AS hr_cost,
        COALESCE(e.warehouse_cost, 0) AS warehouse_cost,
        COALESCE(e.tech_tool_cost, 0) AS tech_tool_cost,
        COALESCE(e.other_cost, 0) AS other_cost,
        COALESCE(e.total_expenses, 0) AS total_other_expenses,

        COALESCE(s.employee_salary, 0) + COALESCE(e.total_expenses, 0) AS total_cost,

        COALESCE(r.order_revenue, 0)
        - COALESCE(s.employee_salary, 0)
        - COALESCE(e.total_expenses, 0) AS total_income

    FROM date_spine d
    LEFT JOIN daily_revenue r
        ON d.date = r.date
    LEFT JOIN daily_salary s
        ON d.date = s.date
    LEFT JOIN daily_expenses e
        ON d.date = e.date

)

SELECT *
FROM final
ORDER BY date