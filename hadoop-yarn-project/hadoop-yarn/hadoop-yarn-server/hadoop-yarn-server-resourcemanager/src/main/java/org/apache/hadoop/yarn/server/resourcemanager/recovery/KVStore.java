package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;

/**
 * Interface to RM Databases (State Store, Conf Store) which provide a KV access
 * This interface can eventually be used by RMStateStore, YarnConfigurationStore which provide
 * application specific logic on top of general KV store
 *
 * Implementations / APIs of this interface should not be tied to any application logic &
 * should be reusable across KV use cases
 *
 * It is acceptable for this to be tied to the Hadoop ecosystem (FileSystem, Conf, etc) as long as it
 * solves the purpose to be reusable across use cases without being tied to app logic
 */
public interface KVStore {

  public void init() throws IOException;

  public byte[] get(byte[] key) throws IOException;

  public void set(byte[] key, byte[] value) throws IOException;

  public void del(byte[] key) throws IOException;

  public void close() throws IOException;

  // TODO - Add iterator / range APIs with a start key / prefix and a limit / end key

}
