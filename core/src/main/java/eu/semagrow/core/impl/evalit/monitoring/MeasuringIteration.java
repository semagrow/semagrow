package eu.semagrow.core.impl.evalit.monitoring;

import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.common.iteration.IterationWrapper;

/**
 * Created by angel on 6/14/14.
 */
public class MeasuringIteration<E,X extends Exception> extends IterationWrapper<E,X> {

    // rate = rows / time in millis
    private double currentConsumedRate = 0;

    private double currentProducedRate = 0;

    private int currentRows = 0;

    private long startTime;

    private long endTime;

    private long lastEnd;

    private long consumedTotalTime = 0;

    private long producedTotalTime = 0;

    public MeasuringIteration(Iteration<E, X> iter) { super(iter); init(); }

    public double getCurrentProducedRate() { return currentProducedRate; }

    public double getCurrentConsumedRate() { return currentConsumedRate; }

    public double getAverageProducedRate() {
        if (producedTotalTime == 0)
            return 0;
        else
            return currentRows / producedTotalTime;
    }

    public double getAverageConsumedRate() {
        if (consumedTotalTime == 0)
            return 0;
        else
            return currentRows / consumedTotalTime;
    }

    public long getCount() { return currentRows; }

    public long getRunningTime() {
        if (isClosed())
            return endTime - startTime;
        else
            return System.currentTimeMillis() - startTime;
    }

    public long getStartTime() { return startTime; }

    public long getEndTime() { return endTime; }

    private void init() { startTime = System.currentTimeMillis(); }

    @Override
    public E next() throws X {

        long currentStartTime = System.currentTimeMillis();

        E item = super.next();

        updateStatistics(currentStartTime, System.currentTimeMillis());

        return item;
    }

    protected void updateStatistics(long start, long end) {
        long currentConsumedTime, currentProducedTime;
        currentRows++;
        if (lastEnd != 0) {
            currentConsumedTime = end - lastEnd;
            consumedTotalTime += currentConsumedTime;
            currentConsumedRate = computeCurrentRate(currentConsumedTime);
        }
        currentProducedTime = end - start;
        producedTotalTime += currentProducedTime;
        lastEnd = end;
        currentProducedRate = computeCurrentRate(currentProducedTime);
    }

    protected double computeCurrentRate(long lastElapsed) {
        return (double)1/lastElapsed;
    }

    @Override
    public void handleClose() throws X {
        super.handleClose();
        endTime = System.currentTimeMillis();
    }
}

