package eu.semagrow.stack.modules.logging;

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
