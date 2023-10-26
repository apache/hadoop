package org.apache.hadoop.fs.azurebfs.security;

import java.io.IOException;

public class NoEncryptionAdapter extends EncryptionAdapter {

  private NoEncryptionAdapter() {

  }
  private static NoEncryptionAdapter instance = new NoEncryptionAdapter();

  public static NoEncryptionAdapter getInstance() {
    return instance;
  }

  @Override
  public String getEncodedKey() throws IOException {
    return null;
  }

  @Override
  public String getEncodedKeySHA() throws IOException {
    return null;
  }

  @Override
  public String getEncodedContext() throws IOException {
    return null;
  }

  @Override
  public void destroy() {

  }
}
