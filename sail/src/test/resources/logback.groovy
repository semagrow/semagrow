def HOME = System.getProperty( "user.home" )
def LOGDIR = "${HOME}/var/log";

appender( "PROCFLOW", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "%level - %date{ISO8601} - %logger - %X{uuid} - %X{nestingLevel} - %msg%n"
  }
}

appender( "CONSOLE", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "%level - %date{ISO8601} - %logger - %msg%n"
  }
}

appender( "FILE", FileAppender ) {
  file = "${LOGDIR}/semagrow.log"
  append = true
  encoder( PatternLayoutEncoder ) {
    pattern = "%level - %date{ISO8601} - %X{uuid} - %logger - %X{nestingLevel} - %msg%n"
  }
}

logger( "eu.semagrow.core.impl", INFO, ["PROCFLOW"], false )

root( WARN, ["CONSOLE"] )
