package org.apache.hadoop.test.system;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Writable;
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
  
  /**
   * Enables a particular control action to be performed on the Daemon <br/>
   * 
   * @param control action to be enabled.
   * 
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  void sendAction(ControlAction action) throws IOException;
  
  /**
   * Checks if the particular control action has be delivered to the Daemon 
   * component <br/>
   * 
   * @param action to be checked.
   * 
   * @return true if action is still in waiting queue of 
   *          actions to be delivered.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  boolean isActionPending(ControlAction action) throws IOException;
  
  /**
   * Removes a particular control action from the list of the actions which the
   * daemon maintains. <br/>
   * <i><b>Not to be directly called by Test Case or clients.</b></i>
   * @param action to be removed
   * @throws IOException
   */
  
  @SuppressWarnings("unchecked")
  void removeAction(ControlAction action) throws IOException;
  
  /**
   * Clears out the list of control actions on the particular daemon.
   * <br/>
   * @throws IOException
   */
  void clearActions() throws IOException;
  
  /**
   * Gets a list of pending actions which are targeted on the specified key. 
   * <br/>
   * <i><b>Not to be directly used by clients</b></i>
   * @param key target
   * @return list of actions.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  ControlAction[] getActions(Writable key) throws IOException;
}