package org.apache.hadoop.dfs;

import java.io.IOException;

/**
 * This exception is thrown when the name node is in safe mode.
 * Client cannot modified namespace until the safe mode is off. 
 * 
 * @author Konstantin Shvachko
 */
public class SafeModeException extends IOException {

  public SafeModeException( String text, FSNamesystem.SafeModeInfo mode  ) {
    super( text + ". Name node is in safe mode.\n" + mode.getTurnOffTip());
  }

}
