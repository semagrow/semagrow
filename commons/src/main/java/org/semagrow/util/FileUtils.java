package org.semagrow.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class FileUtils {
    
    private static final String SEMAGROW_CONFIG = "/etc/default/semagrow/";
    private static final String SEMAGROW_CONFIG_FALLBACK = "/tmp/";
    
    public static File getFile(String fileName) throws IOException{
        File f = new File(SEMAGROW_CONFIG, fileName);
        if(!f.exists()){
            f = new File(SEMAGROW_CONFIG_FALLBACK, fileName);
            if(!f.exists()){
                try {
                    f = new File(FileUtils.class.getClassLoader().getResource(fileName).toURI());
                } catch (URISyntaxException ex) {
                    throw new IOException(ex);
                }
            }
        }
        if(f.canRead()){
            return f;
        } else {
            throw new IOException(fileName + " cannot be read");
        }
    }
}
