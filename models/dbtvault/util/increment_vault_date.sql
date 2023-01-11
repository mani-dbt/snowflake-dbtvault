{{
    config(
        post_hook="{{ increment_vault_date_id() }}"
    )
}}

SELECT 1 as id