{#
This macro returns the payment type information
#}

{% macro get_payment_type_description(type) -%}
    case {{type}}
        when 1 then 'Credit Card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end
{%- endmacro %}