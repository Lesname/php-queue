CREATE TABLE IF NOT EXISTS queue_job
(
    id SERIAL PRIMARY KEY,

    name VARCHAR(50) NOT NULL,
    state VARCHAR(10) NOT NULL,
    data TEXT NOT NULL,
    attempt SMALLINT NOT NULL DEFAULT 0,
    until INT,
    priority SMALLINT,

    reserved_on INT DEFAULT NULL,
    reserved_release INT DEFAULT NULL,
    reserved_key VARCHAR(10) DEFAULT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS reserve ON queue_job (state, until);


create or replace function queue_job_notify_trigger() returns trigger as $$
declare
    payload json;
begin
    if NEW.state = 'ready' THEN
        if new.until IS NULL OR new.until <= extract(epoch from now()) THEN
            payload = to_jsonb(NEW.*);
            perform pg_notify('queue_job_inserted', payload::text);
        end if;
    end if;

    return null;
end;
$$ language plpgsql;

create trigger my_trigger
    after insert on queue_job
    for each row execute function queue_job_notify_trigger();
