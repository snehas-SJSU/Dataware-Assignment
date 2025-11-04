select
  u.userId,
  u.sessionId,
  u.channel,
  st.ts
from {{ ref('user_session_channel') }} u
join {{ ref('session_timestamp') }} st
  on u.sessionId = st.sessionId