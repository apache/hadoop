package org.apache.hadoop.fs.azurebfs.extensions;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
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
  public ByteArrayInputStream getEncryptionContext(String path)
      throws IOException {
    String newContext = UUID.randomUUID().toString();
    pathToContextMap.put(path, newContext);
    //    String key = UUID.randomUUID().toString();
    String key = dummyKey; // replace with above once server supports
    contextToKeyMap.put(newContext, key);
    return new ByteArrayInputStream(newContext.getBytes((StandardCharsets.UTF_8)));
  }

  @Override
  public ByteArrayInputStream getEncryptionKey(String path,
      String encryptionContext) throws IOException {
    if (!encryptionContext.equals(pathToContextMap.get(path))) {
      throw new IOException("encryption context does not match path");
    }
    return new ByteArrayInputStream(contextToKeyMap.get(encryptionContext)
        .getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void close() throws IOException {

  }
}
