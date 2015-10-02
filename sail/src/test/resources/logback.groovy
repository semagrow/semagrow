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

appender( "FILE", FileAppender ) {
  file = "${LOGDIR}/semagrow.log"
  append = true
  encoder( PatternLayoutEncoder ) {
    pattern = "%.-1level - %date{ISO8601} - %X{uuid} - %-10.10logger{0} - %X{nestingLevel} - %msg%n"
  }
}

logger( "eu.semagrow.core.impl", DEBUG, ["PROCFLOW"], false )
logger( "reactor", TRACE, ["PROCFLOW"], false )

root( INFO, ["CONSOLE"] )
