{% for i in range(3) %}
    
    select * from {{ ref('inserttime') }}

{% endfor %}