import ch.qos.logback.core.db.DriverManagerConnectionSource
import ch.qos.logback.classic.db.DBAppender
import eu.semagrow.art.DbAppender

def LOGDIR = "/var/log/semagrow";

appender( "PROCFLOW", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "%.-1level - %date{ISO8601} - [%10.10thread] - %20.-20logger{0} - %.12X{uuid} - %X{nestingLevel} - %msg%n"
  }
}

appender( "CONSOLE", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "%.-1level - %date{ISO8601} - [%10.10thread] - %20.-20logger{0} - %msg%n"
  }
}

appender( "DB", DbAppender) {
  connectionSource(DriverManagerConnectionSource) {
    driverClass = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/logging"
    user = "postgres"
    password = "postgres"
  }
  sqlDialect(PostgreSQLDialect)
}

appender( "FILE", FileAppender ) {
  file = "${LOGDIR}/semagrow.log"
  append = true
  encoder( PatternLayoutEncoder ) {
    pattern = "%.-1level - %date{ISO8601} - %X{uuid} - %-10.10logger{0} - %X{nestingLevel} - %msg%n"
  }
}

logger( "eu.semagrow.core.impl", DEBUG, ["PROCFLOW"], false )
logger( "eu.semagrow.query.impl", DEBUG, ["PROCFLOW"], false )
logger( "eu.semagrow.sail", DEBUG, ["PROCFLOW"], false )

logger( "eu.semagrow.query.impl.SemagrowSailTupleQuery",  INFO, ["DB"], false )
logger( "eu.semagrow.core.impl.planner.DPQueryDecomposer", INFO, ["DB"], false )
logger( "eu.semagrow.core.impl.evaluation.rx.reactor.QueryExecutorImpl", INFO, ["DB"], false )
logger( "eu.semagrow.core.impl.evaluation.rx.LoggingTupleQueryResultHandler", INFO, ["DB"], false )

logger( "reactor", TRACE, ["PROCFLOW"], false )

root( INFO, ["CONSOLE"] )
