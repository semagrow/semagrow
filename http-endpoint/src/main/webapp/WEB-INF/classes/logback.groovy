
appender( "PROCFLOW", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "%.-1level - %date{ISO8601} - [%10.10thread] - %20.-20logger{0} - %.12X{uuid} - %msg%n"
  }
}

appender( "CONSOLE", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "%.-1level - %date{ISO8601} - [%10.10thread] - %20.-20logger{0} - %msg%n"
  }
}


logger( "org.semagrow", WARN , ["PROCFLOW"], false )
logger( "org.semagrow.plan", INFO, ["PROCFLOW"], false)
logger( "org.semagrow.estimator", WARN, ["PROCFLOW"], false)
logger( "org.semagrow.evaluation", INFO, ["PROCFLOW"], false)
logger( "org.semagrow.connector.sparql", INFO, ["PROCFLOW"], false)
logger( "org.semagrow.connector.sparql.execution.TupleQueryResultPublisher", WARN, ["PROCFLOW"], false)
logger( "org.semagrow.http", INFO, ["PROCFLOW"], false )
logger( "org.semagrow.query", INFO, ["PROCFLOW"], false )
logger( "org.semagrow.sail", INFO, ["PROCFLOW"], false )
logger( "org.semagrow.http.views.TupleQueryResultView", OFF, ["PROCFLOW"], false )
logger( "org.semagrow.geospatial", INFO, ["PROCFLOW"], false)

logger( "reactor", WARN, ["PROCFLOW"], false )

root( INFO, ["CONSOLE"] )
