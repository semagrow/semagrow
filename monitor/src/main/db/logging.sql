DROP TABLE    logging_event;
DROP SEQUENCE logging_event_id_seq;


CREATE SEQUENCE logging_event_id_seq MINVALUE 1 START 1;


CREATE TABLE logging_event
  (
    timestmp         BIGINT NOT NULL,
    formatted_message  TEXT NOT NULL,
    logger_name       VARCHAR(254) NOT NULL,
    level_string      VARCHAR(254) NOT NULL,
    thread_name       VARCHAR(254),
    reference_flag    SMALLINT,
    arg0              VARCHAR(254),
    arg1              VARCHAR(254),
    arg2              VARCHAR(254),
    arg3              VARCHAR(254),
    query_id          VARCHAR(254),
    nesting           VARCHAR(254),
    event_id          BIGINT DEFAULT nextval('logging_event_id_seq') PRIMARY KEY
  );

