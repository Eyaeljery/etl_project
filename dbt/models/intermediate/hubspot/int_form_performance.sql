{{ config(
    materialized = 'view'
) }}

with form_submissions as (
    select
        submission_id,
        form_id,
        page_url,
        submitted_at,
        email
    from {{ ref('int_form_submissions') }}
),

contacts as (
    select
        contact_pk,
        email,
        became_lead_at,
        became_customer_at
    from {{ ref('int_contacts_base') }}
),

joined as (
    select
        fs.submission_id,
        fs.form_id,
        fs.page_url,
        fs.submitted_at,
        lower(fs.email)              as email_normalized,
        c.contact_pk,
        c.became_lead_at,
        c.became_customer_at
    from form_submissions fs
    left join contacts c
        on lower(fs.email) = lower(c.email)
),

agg as (
    select
        form_id,
        -- tu peux aussi garder page_url si tu veux distinguer par page
        page_url,

        count(*)                                      as submissions_count,
        count(distinct email_normalized)              as distinct_contacts_count,

        -- nb de leads générés : contacts qui ont une date de passage à lead
        count(distinct contact_pk) filter (
            where became_lead_at is not null
        )                                             as leads_generated_count,

        -- nb de clients générés : contacts qui ont une date de passage à customer
        count(distinct contact_pk) filter (
            where became_customer_at is not null
        )                                             as customers_generated_count
    from joined
    group by form_id, page_url
)

select *
from agg