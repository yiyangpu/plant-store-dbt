WITH cleaned_expenses AS (
    SELECT
        DATE(EXPENSE_DATE) AS expense_date,
        LOWER(EXPENSE_TYPE) AS expense_type,
        EXPENSE_AMOUNT
    FROM {{ ref('base_google_drive_expenses') }}
)

SELECT
    expense_date,
    expense_type,
    SUM(EXPENSE_AMOUNT) AS total_expense
FROM cleaned_expenses
GROUP BY 1,2