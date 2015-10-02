package eu.semagrow.art;


import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Created by angel on 2/10/2015.
 */
@Aspect
public class MethodLogger {


    @Around("execution(* *(..)) && @annotation(Loggable)")
    public Object around(ProceedingJoinPoint point) {


        Object result = null;
        try {
            Method method = MethodSignature.class.cast(point.getSignature()).getMethod();
            Logger logger = LoggerFactory.getLogger(method.getDeclaringClass());
            LogExprProcessing event = new LogExprProcessing();
            logger.info( "Enter {}", method.getName());
            result = point.proceed();
            logger.info( "Exit  {}", method.getName());
            event.finalize();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        /*
                MethodSignature.class.cast(point.getSignature()).getMethod().getName(),
                point.getArgs(),
                result,

                );
        */
        return result;
    }

}
