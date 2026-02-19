SELECT dates, hours, job_id, campaign_id, clicks, spend_hour, conversion
FROM mydb.events
ORDER BY dates DESC, hours DESC
LIMIT 10;

SELECT * FROM mydb.events_metadata ORDER BY created_at DESC LIMIT 10;