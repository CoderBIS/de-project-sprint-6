with user_group_messages as (
    select  luga.hk_group_id,
			count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
from ANTONNN1989GMAILCOM__DWH.l_user_group_activity as luga
join ANTONNN1989GMAILCOM__DWH.l_user_message as lum on luga.hk_user_id=lum.hk_user_id
group by luga.hk_group_id
)
select hk_group_id,
	   cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10
;