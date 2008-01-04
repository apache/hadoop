/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.dfs;

import java.io.*;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.dfs.FSConstants.UpgradeAction;
import org.apache.hadoop.fs.permission.*;

/**********************************************************************
 * ClientProtocol is used by a piece of DFS user code to communicate 
 * with the NameNode.  User code can manipulate the directory namespace, 
 * as well as open/close file streams, etc.
 *
 **********************************************************************/
interface ClientProtocol extends VersionedProtocol {

  /**
   * Compared to the previous version the following changes have been introduced:
   * 16 : removed deprecated obtainLock() and releaseLock(). 
   * 17 : getBlockSize replaced by getPreferredBlockSize
   * 18 : datanodereport returns dead, live or all nodes.
   * 19 : rollEditLog() returns a token to uniquely identify the editfile.
   * 20 : getContentLength returns the total size in bytes of a directory subtree
   * 21 : add lease holder as a parameter in abandonBlock(...)
   * 22 : Serialization of FileStatus has changed.
   * 23 : added setOwner(...) and setPermission(...); changed create(...) and mkdir(...)
   */
  public static final long versionID = 23L;
  
  ///////////////////////////////////////
  // File contents
  ///////////////////////////////////////
  /**
   * Open an existing file and get block locations within the specified range. 
   * Return {@link LocatedBlocks} which contains
   * file length, blocks and their locations.
   * DataNode locations for each block are sorted by
   * the distance to the client's address.
   * 
   * The client will then have to contact
   * one of the indicated DataNodes to obtain the actual data.  There
   * is no need to call close() or any other function after
   * calling open().
   * 
   * @param src file name
   * @param offset range start offset
   * @param length range length
   * @return file length and array of blocks with their locations
   * @throws IOException
   */
  public LocatedBlocks open(String src, 
                            long offset,
                            long length) throws IOException;
  
  /**
   * Get locations of the blocks of the specified file within the specified range.
   * DataNode locations for each block are sorted by
   * the proximity to the client.
   * 
   * @see #open(String, long, long)
   * 
   * @param src file name
   * @param offset range start offset
   * @param length range length
   * @return file length and array of blocks with their locations
   * @throws IOException
   */
  public LocatedBlocks  getBlockLocations(String src,
                                          long offset,
                                          long length) throws IOException;

  /**
   * Create a new file.  Get back block and datanode info,
   * which describes where the first block should be written.
   *
   * Successfully calling this method prevents any other 
   * client from creating a file under the given name, but
   * the caller must invoke complete() for the file to be
   * added to the filesystem.
   *
   * Blocks have a maximum size.  Clients that intend to
   * create multi-block files must also use reportWrittenBlock()
   * and addBlock().
   *
   * If permission denied,
   * an {@link AccessControlException} will be thrown as an
   * {@link org.apache.hadoop.ipc.RemoteException}.
   *
   * @param src The path of the directory being created
   * @param masked The masked permission
   */
  public void create(String src, 
                     FsPermission masked,
                             String clientName, 
                             boolean overwrite, 
                             short replication,
                             long blockSize
                             ) throws IOException;

  /**
   * Set replication for an existing file.
   * 
   * The NameNode sets replication to the new value and returns.
   * The actual block replication is not expected to be performed during  
   * this method call. The blocks will be populated or removed in the 
   * background as the result of the routine block maintenance procedures.
   * 
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(String src, 
                                short replication
                                ) throws IOException;

  /**
   * Set permissions for an existing file/directory.
   */
  public void setPermission(String src, FsPermission permission
      ) throws IOException;

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   * @param src
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  public void setOwner(String src, String username, String groupname
      ) throws IOException;

  /**
   * If the client has not yet called reportWrittenBlock(), it can
   * give up on it by calling abandonBlock().  The client can then
   * either obtain a new block, or complete or abandon the file.
   *
   * Any partial writes to the block will be garbage-collected.
   */
  public void abandonBlock(Block b, String src, String holder
      ) throws IOException;

  /**
   * A client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing)
   * should call addBlock().  
   *
   * addBlock() returns block and datanode info, just like the initial
   * call to create().  
   *
   * A null response means the NameNode could not allocate a block,
   * and that the caller should try again.
   */
  public LocatedBlock addBlock(String src, String clientName) throws IOException;

  /**
   * A client that wants to abandon writing to the current file
   * should call abandonFileInProgress().  After this call, any
   * client can call create() to obtain the filename.
   *
   * Any blocks that have been written for the file will be 
   * garbage-collected.
   * @param src The filename
   * @param holder The datanode holding the lease
   */
  public void abandonFileInProgress(String src, 
                                    String holder) throws IOException;

  /**
   * The client is done writing data to the given filename, and would 
   * like to complete it.  
   *
   * The function returns whether the file has been closed successfully.
   * If the function returns false, the caller should try again.
   *
   * A call to complete() will not return true until all the file's
   * blocks have been replicated the minimum number of times.  Thus,
   * DataNode failures may cause a client to call complete() several
   * times before succeeding.
   */
  public boolean complete(String src, String clientName) throws IOException;

  /**
   * The client wants to report corrupted blocks (blocks with specified
   * locations on datanodes).
   * @param blocks Array of located blocks to report
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

  ///////////////////////////////////////
  // Namespace management
  ///////////////////////////////////////
  /**
   * Rename an item in the fs namespace
   */
  public boolean rename(String src, String dst) throws IOException;

  /**
   * Remove the given filename from the filesystem
   */
  public boolean delete(String src) throws IOException;

  /**
   * Check whether the given file exists
   */
  public boolean exists(String src) throws IOException;

  /**
   * Check whether the given filename is a directory or not.
   */
  public boolean isDir(String src) throws IOException;

  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * If permission denied,
   * an {@link AccessControlException} will be thrown as an
   * {@link org.apache.hadoop.ipc.RemoteException}.
   *
   * @param src The path of the directory being created
   * @param masked The masked permission of the directory being created
   * @return True if the operation success.
   */
  public boolean mkdirs(String src, FsPermission masked) throws IOException;

  /**
   * Get a listing of the indicated directory
   */
  public DFSFileInfo[] getListing(String src) throws IOException;

  ///////////////////////////////////////
  // System issues and management
  ///////////////////////////////////////

  /**
   * Client programs can cause stateful changes in the NameNode
   * that affect other clients.  A client may obtain a file and 
   * neither abandon nor complete it.  A client might hold a series
   * of locks that prevent other clients from proceeding.
   * Clearly, it would be bad if a client held a bunch of locks
   * that it never gave up.  This can happen easily if the client
   * dies unexpectedly.
   *
   * So, the NameNode will revoke the locks and live file-creates
   * for clients that it thinks have died.  A client tells the
   * NameNode that it is still alive by periodically calling
   * renewLease().  If a certain amount of time passes since
   * the last call to renewLease(), the NameNode assumes the
   * client has died.
   */
  public void renewLease(String clientName) throws IOException;

  /**
   * Get a set of statistics about the filesystem.
   * Right now, only two values are returned.
   * [0] contains the total storage capacity of the system,
   *     in bytes.
   * [1] contains the total used space of the system, in bytes.
   * [2] contains the available storage of the system, in bytes.
   */
  public long[] getStats() throws IOException;

  /**
   * Get a report on the system's current datanodes.
   * One DatanodeInfo object is returned for each DataNode.
   * Return live datanodes if type is LIVE; dead datanodes if type is DEAD;
   * otherwise all datanodes if type is ALL.
   */
  public DatanodeInfo[] getDatanodeReport(FSConstants.DatanodeReportType type)
  throws IOException;

  /**
   * Get the block size for the given file.
   * @param filename The name of the file
   * @return The number of bytes in each block
   * @throws IOException
   */
  public long getPreferredBlockSize(String filename) throws IOException;

  /**
   * Enter, leave or get safe mode.
   * <p>
   * Safe mode is a name node state when it
   * <ol><li>does not accept changes to name space (read-only), and</li>
   * <li>does not replicate or delete blocks.</li></ol>
   * 
   * <p>
   * Safe mode is entered automatically at name node startup.
   * Safe mode can also be entered manually using
   * {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_GET)}.
   * <p>
   * At startup the name node accepts data node reports collecting
   * information about block locations.
   * In order to leave safe mode it needs to collect a configurable
   * percentage called threshold of blocks, which satisfy the minimal 
   * replication condition.
   * The minimal replication condition is that each block must have at least
   * <tt>dfs.replication.min</tt> replicas.
   * When the threshold is reached the name node extends safe mode
   * for a configurable amount of time
   * to let the remaining data nodes to check in before it
   * will start replicating missing blocks.
   * Then the name node leaves safe mode.
   * <p>
   * If safe mode is turned on manually using
   * {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_ENTER)}
   * then the name node stays in safe mode until it is manually turned off
   * using {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_LEAVE)}.
   * Current state of the name node can be verified using
   * {@link #setSafeMode(FSConstants.SafeModeAction) setSafeMode(SafeModeAction.SAFEMODE_GET)}
   * <h4>Configuration parameters:</h4>
   * <tt>dfs.safemode.threshold.pct</tt> is the threshold parameter.<br>
   * <tt>dfs.safemode.extension</tt> is the safe mode extension parameter.<br>
   * <tt>dfs.replication.min</tt> is the minimal replication parameter.
   * 
   * <h4>Special cases:</h4>
   * The name node does not enter safe mode at startup if the threshold is 
   * set to 0 or if the name space is empty.<br>
   * If the threshold is set to 1 then all blocks need to have at least 
   * minimal replication.<br>
   * If the threshold value is greater than 1 then the name node will not be 
   * able to turn off safe mode automatically.<br>
   * Safe mode can always be turned off manually.
   * 
   * @param action  <ul> <li>0 leave safe mode;</li>
   *                <li>1 enter safe mode;</li>
   *                <li>2 get safe mode state.</li></ul>
   * @return <ul><li>0 if the safe mode is OFF or</li> 
   *         <li>1 if the safe mode is ON.</li></ul>
   * @throws IOException
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) throws IOException;

  /**
   * Tells the namenode to reread the hosts and exclude files. 
   * @throws IOException
   */
  public void refreshNodes() throws IOException;


  /**
   * Get the size of the current edit log (in bytes).
   * @return The number of bytes in the current edit log.
   * @throws IOException
   */
  public long getEditLogSize() throws IOException;

  /**
   * Closes the current edit log and opens a new one. The 
   * call fails if the file system is in SafeMode.
   * Returns a unique token to identify this transaction.
   * @throws IOException
   */
  public long rollEditLog() throws IOException;

  /**
   * Rolls the fsImage log. It removes the old fsImage, copies the
   * new image to fsImage, removes the old edits and renames edits.new 
   * to edits. The call fails if any of the four files are missing.
   * @throws IOException
   */
  public void rollFsImage() throws IOException;

  /**
   * Finalize previous upgrade.
   * Remove file system state saved during the upgrade.
   * The upgrade will become irreversible.
   * 
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException;

  /**
   * Report distributed upgrade progress or force current upgrade to proceed.
   * 
   * @param action {@link FSConstants.UpgradeAction} to perform
   * @return upgrade status information or null if no upgrades are in progress
   * @throws IOException
   */
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) 
  throws IOException;

  /**
   * Dumps namenode data structures into specified file. If file
   * already exists, then append.
   * @throws IOException
   */
  public void metaSave(String filename) throws IOException;

  /* Get the file info for a specific file or directory.
   * @param src The string representation of the path to the file
   * @throws IOException if file does not exist
   * @return object containing information regarding the file
   */
  public DFSFileInfo getFileInfo(String src) throws IOException;

  /* Get the total size of all files and directories rooted at
   * the specified directory.
   * @param src The string representation of the path
   * @return size of directory subtree in bytes
   */
  public long getContentLength(String src) throws IOException;
}
