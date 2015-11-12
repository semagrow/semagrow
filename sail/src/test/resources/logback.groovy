import ch.qos.logback.core.db.DriverManagerConnectionSource
import eu.semagrow.art.DbAppender

def HOME = System.getProperty( "user.home" )
def LOGDIR = "${HOME}/var/log";

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
    url = "jdbc:postgresql://127.0.0.1:5678/Logging"
    user = "postgres"
    password = "postgres"
  }
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

logger( "eu.semagrow.core.impl",  INFO, ["DB"], false )
logger( "eu.semagrow.query.impl", INFO, ["DB"], false )

logger( "reactor", TRACE, ["PROCFLOW"], false )

root( INFO, ["CONSOLE"] )
