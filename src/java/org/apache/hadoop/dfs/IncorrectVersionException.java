package org.apache.hadoop.dfs;

import java.io.IOException;

/**
 * The exception is thrown when external version does not match 
 * current version of the appication.
 * 
 * @author Konstantin Shvachko
 */
class IncorrectVersionException extends IOException {

  public IncorrectVersionException( int version, String ofWhat ) {
    super( "Unexpected version " 
        + (ofWhat==null ? "" : "of " + ofWhat) + " reported: "
        + version + ". Expecting = " + FSConstants.DFS_CURRENT_VERSION + "." );
  }

}
