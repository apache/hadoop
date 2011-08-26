package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for a deserializer. Throws an IOException if the serialized data is
 * incomplete or wrong.
 * */
public interface CacheableDeserializer<T extends Cacheable> {
  /**
   * Returns the deserialized object.
   *
   * @return T the deserialized object.
   */
  public T deserialize(ByteBuffer b) throws IOException;
}
