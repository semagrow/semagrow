def HOME = System.getProperty( "user.home" )
def LOGDIR = "${HOME}/var/log";

appender( "CONSOLE", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "XXXX %level %logger - %msg%n"
  }
}

appender( "STDOUT", ConsoleAppender ) {
  encoder( PatternLayoutEncoder) {
    pattern = "YYYY %level %logger - %msg%n"
  }
}

appender( "FILE", FileAppender ) {
  file = "${LOGDIR}/testFile.log"
  append = true
  encoder( PatternLayoutEncoder ) {
    pattern = "%level %logger - %msg%n"
  }
}

logger( "eu.semagrow.core.impl.planner", INFO, ["CONSOLE"], false )

root( WARN, ["STDOUT"] )
