package eu.semagrow.art;

import ch.qos.logback.core.Appender;


public class StructuredFileAppender
	extends ch.qos.logback.core.rolling.RollingFileAppender<StructuredLogItem>
	implements Appender<StructuredLogItem>
{

	@Override
	protected void append( StructuredLogItem logItem )
	{
		// TODO Auto-generated method stub
		
	}

}
