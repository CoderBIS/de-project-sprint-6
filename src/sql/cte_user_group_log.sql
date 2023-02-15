with user_group_log as (
select hk_group_id,
	   count(distinct hk_user_id) as cnt_added_users
from ANTONNN1989GMAILCOM__DWH.l_user_group_activity
where (hk_group_id in (select hk_group_id
						from ANTONNN1989GMAILCOM__DWH.h_groups
						order by registration_dt limit 10))
and (hk_l_user_group_activity in (select hk_l_user_group_activity
									from ANTONNN1989GMAILCOM__DWH.s_auth_history
									where event = 'add'))
				group by hk_group_id)
select hk_group_id,
       cnt_added_users
from user_group_log
order by cnt_added_users;