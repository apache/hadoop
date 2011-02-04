package org.apache.hadoop.hbase.coprocessor;

public class GenericEndpoint extends BaseEndpointCoprocessor implements
    GenericProtocol {

  @Override
  public <T> T doWork(T genericObject) {
    return genericObject;
  }

}
