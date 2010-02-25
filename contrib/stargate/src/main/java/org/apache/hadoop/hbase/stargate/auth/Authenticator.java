package org.apache.hadoop.hbase.stargate.auth;

import java.io.IOException;

public abstract class Authenticator {

  public abstract User getUserForToken(String token) throws IOException;

}
