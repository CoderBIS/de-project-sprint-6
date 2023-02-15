INSERT INTO ANTONNN1989GMAILCOM__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select
distinct hash(hg.hk_group_id,hu.hk_user_id),
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from ANTONNN1989GMAILCOM__STAGING.group_log as gl
left join ANTONNN1989GMAILCOM__DWH.h_users as hu on gl.user_id = hu.user_id
left join ANTONNN1989GMAILCOM__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_user_group_activity from ANTONNN1989GMAILCOM__DWH.l_user_group_activity);