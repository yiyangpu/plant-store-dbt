WITH cleaned_expenses AS (
    SELECT
        DATE(EXPENSE_DATE) AS expense_date,
        LOWER(TRIM(EXPENSE_TYPE)) AS expense_type,
        EXPENSE_AMOUNT
    FROM {{ ref('base_google_drive_expenses') }}
)

SELECT
    expense_date,
    SUM(CASE WHEN expense_type = 'hr' THEN expense_amount ELSE 0 END) AS hr_cost,
    SUM(CASE WHEN expense_type = 'warehouse' THEN expense_amount ELSE 0 END) AS warehouse_cost,
    SUM(CASE WHEN expense_type = 'tech tool' THEN expense_amount ELSE 0 END) AS tech_tool_cost,
    SUM(CASE WHEN expense_type = 'other' THEN expense_amount ELSE 0 END) AS other_cost,
    SUM(expense_amount) AS total_expense
FROM cleaned_expenses
GROUP BY expense_date