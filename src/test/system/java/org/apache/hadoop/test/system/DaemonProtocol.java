package org.apache.hadoop.test.system;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * RPC interface of a given Daemon.
 */
public interface DaemonProtocol extends VersionedProtocol{
  long versionID = 1L;

  /**
   * Returns the Daemon configuration.
   * @return Configuration
   * @throws IOException
   */
  Configuration getDaemonConf() throws IOException;

  /**
   * Check if the Daemon is alive.
   * 
   * @throws IOException
   *           if Daemon is unreachable.
   */
  void ping() throws IOException;

  /**
   * Check if the Daemon is ready to accept RPC connections.
   * 
   * @return true if Daemon is ready to accept RPC connection.
   * @throws IOException
   */
  boolean isReady() throws IOException;

  /**
   * Get system level view of the Daemon process.
   * 
   * @return returns system level view of the Daemon process.
   * 
   * @throws IOException
   */
  ProcessInfo getProcessInfo() throws IOException;

  /**
   * Enable the set of specified faults in the Daemon.<br/>
   * 
   * @param faults
   *          list of faults to be enabled.
   * 
   * @throws IOException
   */
  void enable(List<Enum<?>> faults) throws IOException;

  /**
   * Disable all the faults which are enabled in the Daemon. <br/>
   * 
   * @throws IOException
   */
  void disableAll() throws IOException;

  /**
   * Return a file status object that represents the path.
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  FileStatus getFileStatus(String path, boolean local) throws IOException;

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  FileStatus[] listStatus(String path, boolean local) throws IOException;
}