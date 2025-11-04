-- models/input/session_timestamp.sql
-- Input model built as a CTE (ephemeral via dbt_project.yml)
select
  sessionId,
  ts
from {{ source('raw', 'session_timestamp') }}
where sessionId is not null
