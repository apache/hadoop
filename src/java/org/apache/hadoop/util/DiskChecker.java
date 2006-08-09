package org.apache.hadoop.util;

import java.io.File;
import java.io.IOException;

/**
 * Class that provides utility functions for checking disk problem
 * @author Hairong Kuang
 */

public class DiskChecker {

    public static class DiskErrorException extends IOException {
      public DiskErrorException(String msg) {
        super(msg);
      }
    }
    
    public static class DiskOutOfSpaceException extends IOException {
        public DiskOutOfSpaceException(String msg) {
          super(msg);
        }
      }
      
    public static void checkDir( File dir ) throws DiskErrorException {
        if( !dir.exists() && !dir.mkdirs() )
            throw new DiskErrorException( "can not create directory: " 
                    + dir.toString() );
        
        if ( !dir.isDirectory() )
            throw new DiskErrorException( "not a directory: " 
                    + dir.toString() );
            
        if( !dir.canRead() )
            throw new DiskErrorException( "directory is not readable: " 
                    + dir.toString() );
            
        if( !dir.canWrite() )
            throw new DiskErrorException( "directory is not writable: " 
                    + dir.toString() );
    }

}
