package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.logging;

import com.lmax.disruptor.EventFactory;

public class LogEventFactory implements EventFactory<LogEvent>
{
    public LogEvent newInstance()
    {
        return new LogEvent();
    }
}
