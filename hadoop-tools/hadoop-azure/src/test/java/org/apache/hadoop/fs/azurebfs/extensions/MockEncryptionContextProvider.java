package org.apache.hadoop.fs.azurebfs.extensions;

import org.apache.hadoop.conf.Configuration;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

public class MockEncryptionContextProvider implements EncryptionContextProvider {
  String dummyKey = "12345678901234567890123456789012";//UUID.randomUUID().toString();
  HashMap<String, SecretKey> pathToContextMap = new HashMap<>();
  HashMap<SecretKey, SecretKey> contextToKeyMap = new HashMap<>();
  @Override
  public void initialize(Configuration configuration, String accountName,
      String fileSystem) throws IOException {
    System.out.println("key for this session is " + dummyKey);
  }

  @Override
  public SecretKey getEncryptionContext(String path)
      throws IOException {
    SecretKey newContext = new Key("context".getBytes(StandardCharsets.UTF_8));
//        new Key(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
    pathToContextMap.put(path, newContext);
    //    String key = UUID.randomUUID().toString();
    SecretKey key = new Key(dummyKey.getBytes(StandardCharsets.UTF_8));
    // replace with above once server supports
    contextToKeyMap.put(newContext, key);
    for (SecretKey k : contextToKeyMap.keySet()) {
      System.out.println(new String(k.getEncoded(), StandardCharsets.UTF_8)
          + " .. " + new String(contextToKeyMap.get(k).getEncoded(),
          StandardCharsets.UTF_8));
    }
    return newContext;
  }

  @Override
  public SecretKey getEncryptionKey(String path,
      SecretKey encryptionContext) throws IOException {
//    if (!encryptionContext.equals(pathToContextMap.get(path))) {
//      throw new IOException("encryption context does not match path");
//    }
    for (SecretKey k : contextToKeyMap.keySet()) {
      System.out.println(new String(k.getEncoded(), StandardCharsets.UTF_8)
          + " .. " + new String(contextToKeyMap.get(k).getEncoded(),
          StandardCharsets.UTF_8));
    }
    System.out.println(new String(encryptionContext.getEncoded()));
    System.out.println(contextToKeyMap.containsKey(encryptionContext));
    return contextToKeyMap.get(encryptionContext);
  }

  @Override
  public void destroy() {

  }

  class Key implements SecretKey {

    private final byte[] key;

    Key(byte[] secret) {
      key = secret;
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

    @Override
    public void destroy() {
      Arrays.fill(key, (byte)0);
    }

    @Override
    public boolean equals(Object key) {
      SecretKey k = (SecretKey) key;
      return new String(k.getEncoded()).equals(new String(this.key));
    }
  }
}


