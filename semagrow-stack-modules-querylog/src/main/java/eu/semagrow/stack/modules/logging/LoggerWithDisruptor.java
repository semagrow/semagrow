/**
 * 
 */
package eu.semagrow.stack.modules.logging;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * @author Giannis Mouchakis
 *
 */
public class LoggerWithDisruptor {
	
	private LogEventProducer producer;
	Disruptor<LogEvent> disruptor;
	
	LoggerWithDisruptor() {
		// Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        LogEventFactory factory = new LogEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        this.disruptor = new Disruptor<>(factory, bufferSize, executor);

        // Connect the handler
        disruptor.handleEventsWith(new LogEventHandler());

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<LogEvent> ringBuffer = disruptor.getRingBuffer();

        this.producer = new LogEventProducer(ringBuffer);
	}
	
	public LogEventProducer getProduser() {
		return this.producer;
	}
	
	public void close() {
		disruptor.shutdown();//TODO: check if this closes everything
	}

}
