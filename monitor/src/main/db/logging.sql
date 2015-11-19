DROP TABLE    logging_event;
DROP SEQUENCE logging_event_id_seq;

DROP VIEW query_start;
DROP VIEW decomposition_end;
DROP VIEW execution_end;
DROP VIEW source_query_start;
DROP VIEW source_query_end;
DROP VIEW query;
DROP VIEW evaluation;
DROP VIEW decomposition;
DROP VIEW source_query;


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


CREATE VIEW query_start AS
  SELECT
    timestmp as query_start_time,
    query_id,
    arg0 as query_string
  FROM
    logging_event
  WHERE
    logger_name='eu.semagrow.query.impl.SemagrowSailTupleQuery'
    and arg0 is not null;


CREATE VIEW decomposition_end AS
  SELECT
    timestmp as decomposition_end_time,
    query_id
  FROM
    logging_event
  WHERE
    logger_name='eu.semagrow.core.impl.planner.DPQueryDecomposer'
    and formatted_message='Exit  decompose';


CREATE VIEW execution_end AS
  SELECT
    timestmp as execution_end_time,
    query_id
  FROM
    logging_event
  WHERE
    logger_name='eu.semagrow.query.impl.SemagrowSailTupleQuery'
    and formatted_message='Query evaluation End.';


CREATE VIEW source_query_start AS
  SELECT
    timestmp as begin_time,
    arg1 as source_query_id,
    arg3 as endpoint,
    query_id
  FROM
    logging_event
  WHERE
    logger_name='eu.semagrow.core.impl.evaluation.rx.reactor.QueryExecutorImpl'
    and arg3 is not null;


CREATE VIEW source_query_end AS
  SELECT
    timestmp as end_time,
    arg0 as source_query_id,
    query_id
  FROM
    logging_event
  WHERE
    logger_name='eu.semagrow.core.impl.evaluation.rx.LoggingTupleQueryResultHandler'
    and arg1 is not null;


CREATE VIEW query AS
  SELECT query_id, query_string FROM query_start
  ORDER BY query_start_time;


CREATE VIEW decomposition AS
  SELECT
    query_start.query_id,
    (decomposition_end_time - query_start_time) as decomposition_time
  FROM query_start, decomposition_end
  WHERE query_start.query_id = decomposition_end.query_id;


CREATE VIEW evaluation AS
  SELECT
    query_start.query_id,
    (execution_end_time - query_start_time) as evaluation_time
  FROM query_start, execution_end
  WHERE query_start.query_id = execution_end.query_id;


CREATE VIEW source_query AS
  SELECT
    source_query_start.query_id,
    endpoint,
    sum(end_time - begin_time)
  FROM
    source_query_start,
    source_query_end
  WHERE
    source_query_start.query_id = source_query_end.query_id and
    source_query_start.source_query_id = source_query_end.source_query_id
  GROUP BY
    source_query_start.query_id, endpoint;
