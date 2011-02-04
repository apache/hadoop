package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface GenericProtocol extends CoprocessorProtocol {

  /**
   * Simple interface to allow the passing of a generic parameter to see if the
   * RPC framework can accommodate generics.
   * 
   * @param <T>
   * @param genericObject
   * @return
   */
  public <T> T doWork(T genericObject);

}
