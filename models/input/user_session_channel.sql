-- models/input/user_session_channel.sql
select
  userId,
  sessionId,
  channel
from {{ source('raw', 'user_session_channel') }}
where sessionId is not null
