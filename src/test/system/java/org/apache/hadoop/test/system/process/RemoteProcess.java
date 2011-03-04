package org.apache.hadoop.test.system.process;

import java.io.IOException;

/**
 * Interface to manage the remote process.
 */
public interface RemoteProcess {
  /**
   * Get the host on which the daemon process is running/stopped.<br/>
   * 
   * @return hostname on which process is running/stopped.
   */
  String getHostName();

  /**
   * Start a given daemon process.<br/>
   * 
   * @throws IOException if startup fails.
   */
  void start() throws IOException;

  /**
   * Stop a given daemon process.<br/>
   * 
   * @throws IOException if shutdown fails.
   */
  void kill() throws IOException;

  /**
   * Get the role of the Daemon in the cluster.
   * 
   * @return Enum
   */
  Enum<?> getRole();
}