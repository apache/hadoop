/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/**********************************************************************
 * ClientProtocol is used by a piece of DFS user code to communicate 
 * with the NameNode.  User code can manipulate the directory namespace, 
 * as well as open/close file streams, etc.
 *
 * @author Mike Cafarella
 **********************************************************************/
interface ClientProtocol {

    ///////////////////////////////////////
    // File contents
    ///////////////////////////////////////
    /**
     * Open an existing file, at the given name.  Returns block 
     * and DataNode info.  The client will then have to contact
     * each indicated DataNode to obtain the actual data.  There
     * is no need to call close() or any other function after
     * calling open().
     */
    public LocatedBlock[] open(String src) throws IOException;

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
     */
    public LocatedBlock create(String src, String clientName, String clientMachine, boolean overwrite) throws IOException;

    /**
     * A client that has written a block of data can report completion
     * back to the NameNode with reportWrittenBlock().  Clients cannot
     * obtain an additional block until the previous one has either been 
     * reported as written or abandoned.
     */
    public void reportWrittenBlock(LocatedBlock b) throws IOException;

    /**
     * If the client has not yet called reportWrittenBlock(), it can
     * give up on it by calling abandonBlock().  The client can then
     * either obtain a new block, or complete or abandon the file.
     *
     * Any partial writes to the block will be garbage-collected.
     */
    public void abandonBlock(Block b, String src) throws IOException;

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
    public LocatedBlock addBlock(String src, String clientMachine) throws IOException;

    /**
     * A client that wants to abandon writing to the current file
     * should call abandonFileInProgress().  After this call, any
     * client can call create() to obtain the filename.
     *
     * Any blocks that have been written for the file will be 
     * garbage-collected.
     */
    public void abandonFileInProgress(String src) throws IOException;

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
     * name.
     */
    public boolean mkdirs(String src) throws IOException;

    /**
     * Get a listing of the indicated directory
     */
    public DFSFileInfo[] getListing(String src) throws IOException;

    ///////////////////////////////////////
    // System issues and management
    ///////////////////////////////////////
    /**
     * getHints() returns a list of hostnames that store data for
     * a specific file region.  It returns a set of hostnames for 
     * every block within the indicated region.
     *
     * This function is very useful when writing code that considers
     * data-placement when performing operations.  For example, the
     * MapReduce system tries to schedule tasks on the same machines
     * as the data-block the task processes. 
     */
    public String[][] getHints(String src, long start, long len) throws IOException;
    /**
     * obtainLock() is used for lock managemnet.  It returns true if
     * the lock has been seized correctly.  It returns false if the
     * lock could not be obtained, and the client should try again.
     *
     * Locking is a part of most filesystems and is useful for a
     * number of inter-process synchronization tasks.
     */
    public boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException;

    /**
     * releaseLock() is called if the client would like to release
     * a held lock.  It returns true if the lock is correctly released.
     * It returns false if the client should wait and try again.
     */
    public boolean releaseLock(String src, String clientName) throws IOException;

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
     * [1] contains the available storage of the system, in bytes.
     */
    public long[] getStats() throws IOException;

    /**
     * Get a full report on the system's current datanodes.
     * One DatanodeInfo object is returned for each DataNode.
     */
    public DatanodeInfo[] getDatanodeReport() throws IOException;
}
