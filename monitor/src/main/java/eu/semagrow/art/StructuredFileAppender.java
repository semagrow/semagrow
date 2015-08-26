package eu.semagrow.art;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.encoder.Encoder;


public class StructuredFileAppender
	extends ch.qos.logback.core.rolling.RollingFileAppender<StructuredLogItem>
	implements Appender<StructuredLogItem>
{

	StructuredFileAppender()
	{
		super();
		Encoder<StructuredLogItem> encoder = null;
		this.setEncoder( encoder );
	}


}
