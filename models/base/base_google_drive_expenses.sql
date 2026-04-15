SELECT
    DATE AS expense_date,

    TO_NUMBER(REGEXP_REPLACE(EXPENSE_AMOUNT, '[^0-9.]', '')) AS expense_amount,

    EXPENSE_TYPE

FROM {{ source('google_drive', 'expenses') }}