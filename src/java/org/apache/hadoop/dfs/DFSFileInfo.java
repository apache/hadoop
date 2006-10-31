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

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;

import java.io.*;

/******************************************************
 * DFSFileInfo tracks info about remote files, including
 * name, size, etc.  
 * 
 * @author Mike Cafarella
 ******************************************************/
class DFSFileInfo implements Writable {
    static {                                      // register a ctor
      WritableFactories.setFactory
        (DFSFileInfo.class,
         new WritableFactory() {
           public Writable newInstance() { return new DFSFileInfo(); }
         });
    }

    UTF8 path;
    long len;
    long contentsLen;
    boolean isDir;
    short blockReplication;
    long blockSize;

    /**
     */
    public DFSFileInfo() {
    }

    /**
     * Create DFSFileInfo by file INode 
     */
    public DFSFileInfo( FSDirectory.INode node ) {
        this.path = new UTF8(node.computeName());
        this.isDir = node.isDir();
        if( isDir ) {
          this.len = 0;
          this.contentsLen = node.computeContentsLength();
        } else 
          this.len = this.contentsLen = node.computeFileLength();
        this.blockReplication = node.getReplication();
        blockSize = node.getBlockSize();
    }

    /**
     */
    public String getPath() {
        return path.toString();
    }

    /**
     */
    public String getName() {
        return new Path(path.toString()).getName();
    }

    /**
     */
    public String getParent() {
      return new Path(path.toString()).getParent().toString();
    }

    /**
     */
    public long getLen() {
        return len;
    }

    /**
     */
    public long getContentsLen() {
        return contentsLen;
    }

    /**
     */
    public boolean isDir() {
        return isDir;
    }

    /**
     */
    public short getReplication() {
      return this.blockReplication;
    }

    /**
     * Get the block size of the file.
     * @return the number of bytes
     */
    public long getBlockSize() {
      return blockSize;
    }
    
    //////////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        path.write(out);
        out.writeLong(len);
        out.writeLong(contentsLen);
        out.writeBoolean(isDir);
        out.writeShort(blockReplication);
        out.writeLong(blockSize);
    }

    public void readFields(DataInput in) throws IOException {
        this.path = new UTF8();
        this.path.readFields(in);
        this.len = in.readLong();
        this.contentsLen = in.readLong();
        this.isDir = in.readBoolean();
        this.blockReplication = in.readShort();
        blockSize = in.readLong();
    }
}

