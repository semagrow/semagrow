package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.logging;

import com.lmax.disruptor.RingBuffer;

public class LogEventProducer
{
    private final RingBuffer<LogEvent> ringBuffer;

    public LogEventProducer(RingBuffer<LogEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    public void onData(Object obj)
    {
        long sequence = ringBuffer.next();  // Grab the next sequence
        try
        {
            LogEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
                                                        // for the sequence
            event.set(obj);  // Fill with data
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }
}