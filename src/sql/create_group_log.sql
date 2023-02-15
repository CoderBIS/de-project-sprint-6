create table if not exists ANTONNN1989GMAILCOM__STAGING.group_log
(
    group_id        int REFERENCES ANTONNN1989GMAILCOM__STAGING.groups(id),
    user_id         int REFERENCES ANTONNN1989GMAILCOM__STAGING.users(id),
    user_id_from    int,
    event           varchar(30),
    event_dt        timestamp
)
ORDER BY group_id
SEGMENTED BY group_id all nodes
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);