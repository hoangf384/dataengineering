create table recruitment.tracking
(
    create_time  timeuuid PRIMARY KEY,
    bid          int,
    bn           text,
    campaign_id  int,
    cd           int,
    custom_track text,
    de           text,
    dl           text,
    dt           text,
    ed           text,
    ev           int,
    group_id     int,
    id           text,
    job_id       int,
    md           text,
    publisher_id int,
    rl           text,
    sr           text,
    ts           text,
    tz           int,
    ua           text,
    uid          text,
    utm_campaign text,
    utm_content  text,
    utm_medium   text,
    utm_source   text,
    utm_term     text,
    v            int,
    vp           text
);

-- select * from recruitment.tracking limit 100 allow filtering;