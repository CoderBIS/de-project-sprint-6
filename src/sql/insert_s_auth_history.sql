INSERT INTO ANTONNN1989GMAILCOM__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select
           hash(gl.group_id,gl.user_id),
           gl.user_id_from,
           gl.event,
           gl.event_dt,
           now() as load_dt,
           's3' as load_src
from ANTONNN1989GMAILCOM__STAGING.group_log as gl
left join ANTONNN1989GMAILCOM__DWH.s_auth_history as sah on hash(gl.group_id,gl.user_id)=sah.hk_l_user_group_activity
where sah.hk_l_user_group_activity is null or ((hash(gl.group_id,gl.user_id) <> sah.hk_l_user_group_activity) and (gl.user_id_from <> sah.user_id_from) and (gl.event_dt <> sah.event_dt));