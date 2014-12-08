package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.logging;

public class LogEvent
{
    private Object obj;

    public void set(Object obj)
    {
        this.obj = obj;
    }
    
    public Object get()
    {
        return this.obj;
    }
}
