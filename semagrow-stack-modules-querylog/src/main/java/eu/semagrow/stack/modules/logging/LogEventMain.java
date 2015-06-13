package eu.semagrow.stack.modules.logging;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.RingBuffer;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LogEventMain
{
    public static void main(String[] args) throws Exception
    {
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        LogEventFactory factory = new LogEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        Disruptor<LogEvent> disruptor = new Disruptor<>(factory, bufferSize, executor);

        // Connect the handler
        disruptor.handleEventsWith(new LogEventHandler());

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<LogEvent> ringBuffer = disruptor.getRingBuffer();

        LogEventProducer producer = new LogEventProducer(ringBuffer);

		long startTime = System.nanoTime();

		Integer i = 0;
		/*while (i < 10000000) {*/
		while (i < 100000000) {
			String str = i.toString();
			producer.onData(str);
			i++;
		}

		long endTime = System.nanoTime();
		long duration = endTime - startTime;

		System.out.format("duration=%d seconds%n", TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS));
		
		disruptor.shutdown();
        
        /*ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++)
        {
            bb.putLong(0, l);
            producer.onData(bb);
            Thread.sleep(1000);
        }*/
    }
}
