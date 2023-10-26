package org.apache.hadoop.fs.azurebfs.security;

import java.io.IOException;

public abstract class EncryptionAdapter {
  public abstract String getEncodedKey() throws IOException;

  public abstract String getEncodedKeySHA() throws IOException;

  public abstract String getEncodedContext() throws IOException;

  public abstract void destroy();
}
