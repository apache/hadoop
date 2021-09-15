package org.apache.hadoop.fs.azurebfs.extensions;

import org.apache.avro.util.ByteBufferInputStream;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface EncryptionContextProvider extends AutoCloseable {
  /**
   * Initialize instance
   *
   * @param configuration rawConfig instance
   * @param accountName Account Name (with domain)
   * @throws IOException error in initialization
   */
  void initialize(Configuration configuration, String accountName, String fileSystem) throws IOException;

  /**
   * Fetch encryption context for a given path
   *
   * @param path file path from filesystem root
   * @return encryptionContext string
   * @throws IOException
   */
  ByteBufferInputStream getEncryptionContext(String path) throws IOException;

  /**
   * Fetch encryption key in-exchange for encryption context
   *
   * @param path file path from filesystem root
   * @param encryptionContext encryptionContext fetched from server
   * @return Encryption key
   * @throws IOException
   */
  ByteBufferInputStream getEncryptionKey(String path, String encryptionContext) throws IOException;

  @Override
  void close() throws IOException;
}
