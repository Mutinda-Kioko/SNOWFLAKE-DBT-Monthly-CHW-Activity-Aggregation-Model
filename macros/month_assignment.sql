{% macro month_assignment(date_column) %}
{#
Macro: month_assignment
Description: Assigns activities to the correct reporting month based on the 26th cutoff rule
Parameters:
- date_column: The name of the date column to process (e.g., 'activity_date')
Returns:
- DATE: The first day of the assigned reporting month (YYYY-MM-01 format)
#}

    CASE 
        WHEN {{ date_column }} IS NULL THEN NULL
        WHEN EXTRACT(DAY FROM {{ date_column }}) >= 26 
            THEN DATE_TRUNC('month', DATEADD('month', 1, {{ date_column }}))
        ELSE 
            DATE_TRUNC('month', {{ date_column }})
    END

{% endmacro %}