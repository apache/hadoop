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
import org.apache.hadoop.io.*;

/**********************************************************************
 * Protocol that an DFS client uses to communicate with the NameNode.
 * It's used to manipulate the namespace, and obtain datanode info.
 *
 * @author Mike Cafarella
 **********************************************************************/
public interface ClientProtocol {

    /**
     * Open an existing file.  Get back block and datanode info
     */
    public LocatedBlock[] open(String src) throws IOException;

    /**
     * Create a new file.  Get back block and datanode info
     */
    public LocatedBlock create(String src, String clientName, boolean overwrite) throws IOException;

    /**
     * The client wants to write an additional block to the indicated
     * filename (which must currently be open for writing).  Return
     * block and datanode info.  A null response means the caller
     * should attempt the call again.
     */
    public LocatedBlock addBlock(String src) throws IOException;

    /**
     * The client wants to report a block it has just successfully
     * written to one or more datanodes.  Client-written blocks are
     * always reported by the client, not by the datanode.
     */
    public void reportWrittenBlock(LocatedBlock b) throws IOException;

    /**
     * The client wants to abandon writing to the indicated block,
     * part of the indicated (currently-open) filename.
     */
    public void abandonBlock(Block b, String src) throws IOException;

    /**
     * The client wants to abandon writing to the current file, and
     * let anyone else grab it.
     */
    public void abandonFileInProgress(String src) throws IOException;

    /**
     * The client is done writing data to the given filename, and would 
     * like to complete it.  Returns whether the file has been closed
     * correctly (true) or whether caller should try again (false).
     * (Because the namenode is waiting for a block to complete).
     */
    public boolean complete(String src, String clientName) throws IOException;
    
    /**
     * The client wants to read the indicated filename at a certain offset.
     * Return a list of hostnames where the data can be found.  (Return
     * a set of hostnames for every block.)
     */
    public String[][] getHints(String src, long start, long len) throws IOException;

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
     * The client is trying to obtain a lock.  Return whether the lock has
     * been seized correctly (true), or whether the client should try again
     * (false).
     */
    public boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException;

    /**
     * The client wants to release a held lock.  Return whether the lock was
     * correctly released (true), or whether the client should wait and try the 
     * call again (false).
     */
    public boolean releaseLock(String src, String clientName) throws IOException;

    /**
     * The client machine wants to obtain a lease
     */
    public void renewLease(String clientName) throws IOException;

    /**
     * Get a listing of the indicated directory
     */
    public DFSFileInfo[] getListing(String src) throws IOException;

    /**
     * Get a set of statistics about the filesystem.
     */
    public long[] getStats() throws IOException;

    /**
     * Get a full report on the system's current datanodes.
     */
    public DatanodeInfo[] getDatanodeReport() throws IOException;
}
