
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

appender( "FILE", FileAppender ) {
  file = "${LOGDIR}/semagrow.log"
  append = true
  encoder( PatternLayoutEncoder ) {
    pattern = "%.-1level - %date{ISO8601} - %X{uuid} - %-10.10logger{0} - %X{nestingLevel} - %msg%n"
  }
}

logger( "org.semagrow.core", DEBUG, ["PROCFLOW"], false )
logger( "org.semagrow.query", DEBUG, ["PROCFLOW"], false )
logger( "org.semagrow.sail", DEBUG, ["PROCFLOW"], false )

logger( "org.semagrow.query.impl.SemagrowSailTupleQuery",  INFO, ["CONSOLE"], false )
logger( "org.semagrow.plan.DPQueryDecomposer", INFO, ["CONSOLE"], false )
logger( "org.semagrow.sparql.execution.SPARQLQueryExecutor", INFO, ["CONSOLE"], false )
logger( "org.semagrow.evaluation.LoggingTupleQueryResultHandler", INFO, ["CONSOLE"], false )

logger( "reactor", TRACE, ["PROCFLOW"], false )

root( INFO, ["CONSOLE"] )
