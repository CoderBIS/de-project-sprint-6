with 
user_group_messages as (
    select  luga.hk_group_id,
			count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
from ANTONNN1989GMAILCOM__DWH.l_user_group_activity as luga
join ANTONNN1989GMAILCOM__DWH.l_user_message as lum on luga.hk_user_id=lum.hk_user_id
group by luga.hk_group_id
),
user_group_log as (
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
select ugl.hk_group_id,
	   ugl.cnt_added_users,
	   ugm.cnt_users_in_group_with_messages,
	   round(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users, 2) as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id=ugm.hk_group_id
order by group_conversion desc;