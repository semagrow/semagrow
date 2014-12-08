package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.logging;

import java.nio.ByteBuffer;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.EventTranslatorOneArg;

public class LogEventProducerWithTranslator
{
    private final RingBuffer<LogEvent> ringBuffer;

    public LogEventProducerWithTranslator(RingBuffer<LogEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<LogEvent, ByteBuffer> TRANSLATOR =
        new EventTranslatorOneArg<LogEvent, ByteBuffer>()
        {
            public void translateTo(LogEvent event, long sequence, ByteBuffer bb)
            {
                event.set(bb.getLong(0));
            }
        };

    public void onData(ByteBuffer bb)
    {
        ringBuffer.publishEvent(TRANSLATOR, bb);
    }
}