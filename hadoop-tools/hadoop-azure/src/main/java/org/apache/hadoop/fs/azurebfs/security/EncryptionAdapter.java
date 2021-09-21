package org.apache.hadoop.fs.azurebfs.security;

import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;
import java.io.IOException;

public class EncryptionAdapter implements Destroyable {
  String path;
  String encryptionContext;
  SecretKey encryptionKey;
  EncryptionContextProvider provider;
  byte[] encodedKey = null;
  byte[] encodedKeySHA = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(EncryptionAdapter.class);

  public EncryptionAdapter(EncryptionContextProvider provider, String path,
      String encryptionContext) throws IOException {
    this(provider, path);
    Preconditions.checkNotNull(encryptionContext,
        "Encryption context should not be null.");
    this.encryptionContext = encryptionContext;
  }

  public EncryptionAdapter(EncryptionContextProvider provider, String path)
      throws IOException {
    this.provider = provider;
    this.path = path;
  }

  public SecretKey getEncryptionKey() throws IOException {
    if (encryptionKey != null) {
      return encryptionKey;
    }
    return provider.getEncryptionKey(path, encryptionContext);
  }

  public String fetchEncryptionContext() throws IOException {
    encryptionContext = provider.getEncryptionContext(path);
    return encryptionContext;
  }

  public byte[] getEncodedKey() {
    return encodedKey;
  }

  public void setEncodedKey(byte[] encodedKey) {
    this.encodedKey = encodedKey;
  }

  public byte[] getEncodedKeySHA() {
    return encodedKeySHA;
  }

  public void setEncodedKeySHA(byte[] encodedKeySHA) {
    this.encodedKeySHA = encodedKeySHA;
  }

  public void destroy() throws DestroyFailedException {
    encryptionKey.destroy();
    provider.destroy();
  }
}
