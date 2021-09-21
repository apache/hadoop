package org.apache.hadoop.fs.azurebfs.extensions;

import org.apache.hadoop.conf.Configuration;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

public class MockEncryptionContextProvider implements EncryptionContextProvider {
  String dummyKey = UUID.randomUUID().toString();
  HashMap<String, String> pathToContextMap = new HashMap<>();
  HashMap<String, String> contextToKeyMap = new HashMap<>();
  @Override
  public void initialize(Configuration configuration, String accountName,
      String fileSystem) throws IOException {

  }

  @Override
  public String getEncryptionContext(String path)
      throws IOException {
    String newContext = UUID.randomUUID().toString();
    pathToContextMap.put(path, newContext);
    //    String key = UUID.randomUUID().toString();
    String key = dummyKey; // replace with above once server supports
    contextToKeyMap.put(newContext, key);
    return newContext;
  }

  @Override
  public SecretKey getEncryptionKey(String path,
      String encryptionContext) throws IOException {
    if (!encryptionContext.equals(pathToContextMap.get(path))) {
      throw new IOException("encryption context does not match path");
    }
    return new Key(encryptionContext);
  }

  @Override
  public void destroy() {

  }

  class Key implements SecretKey {

    private final byte[] key;
    Key(String encryptionContext) {
      key =
          contextToKeyMap.get(encryptionContext).getBytes(StandardCharsets.UTF_8);
    }
    @Override
    public String getAlgorithm() {
      return null;
    }

    @Override
    public String getFormat() {
      return null;
    }

    @Override
    public byte[] getEncoded() {
      return key;
    }
  }
}


