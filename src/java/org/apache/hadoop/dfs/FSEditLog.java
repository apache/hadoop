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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 * @author Konstantin Shvachko
 */
class FSEditLog {
  private static final byte OP_ADD = 0;
  private static final byte OP_RENAME = 1;
  private static final byte OP_DELETE = 2;
  private static final byte OP_MKDIR = 3;
  private static final byte OP_SET_REPLICATION = 4;
  private static final byte OP_DATANODE_ADD = 5;
  private static final byte OP_DATANODE_REMOVE = 6;
  
  private File editsFile;
  DataOutputStream editsStream = null;
  
  FSEditLog( File edits ) {
    this.editsFile = edits;
  }
  
  File getEditsFile() {
    return this.editsFile;
  }

  /**
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  void create() throws IOException {
    editsStream = new DataOutputStream(new FileOutputStream(editsFile));
    editsStream.writeInt( FSConstants.DFS_CURRENT_VERSION );
  }
  
  /**
   * Shutdown the filestore
   */
  void close() throws IOException {
    editsStream.close();
  }

  /**
   * Load an edit log, and apply the changes to the in-memory structure
   *
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits( Configuration conf ) throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    int logVersion = 0;
    
    if (editsFile.exists()) {
      DataInputStream in = new DataInputStream(
          new BufferedInputStream(
              new FileInputStream(editsFile)));
      // Read log file version. Could be missing. 
      in.mark( 4 );
      if( in.available() > 0 ) {
        logVersion = in.readByte();
        in.reset();
        if( logVersion >= 0 )
          logVersion = 0;
        else
          logVersion = in.readInt();
        if( logVersion < FSConstants.DFS_CURRENT_VERSION ) // future version
          throw new IOException(
              "Unexpected version of the file system log file: "
              + logVersion
              + ". Current version = " 
              + FSConstants.DFS_CURRENT_VERSION + "." );
      }
      
      short replication = (short)conf.getInt("dfs.replication", 3);
      try {
        while (in.available() > 0) {
          byte opcode = in.readByte();
          numEdits++;
          switch (opcode) {
          case OP_ADD: {
            UTF8 name = new UTF8();
            ArrayWritable aw = null;
            Writable writables[];
            // version 0 does not support per file replication
            if( logVersion >= 0 )
              name.readFields(in);  // read name only
            else {  // other versions do
              // get name and replication
              aw = new ArrayWritable(UTF8.class);
              aw.readFields(in);
              writables = aw.get(); 
              if( writables.length != 2 )
                throw new IOException("Incorrect data fortmat. " 
                    + "Name & replication pair expected");
              name = (UTF8) writables[0];
              replication = Short.parseShort(
                  ((UTF8)writables[1]).toString());
              replication = adjustReplication( replication, conf );
            }
            // get blocks
            aw = new ArrayWritable(Block.class);
            aw.readFields(in);
            writables = aw.get();
            Block blocks[] = new Block[writables.length];
            System.arraycopy(writables, 0, blocks, 0, blocks.length);
            // add to the file tree
            fsDir.unprotectedAddFile(name, blocks, replication );
            break;
          }
          case OP_SET_REPLICATION: {
            UTF8 src = new UTF8();
            UTF8 repl = new UTF8();
            src.readFields(in);
            repl.readFields(in);
            replication = adjustReplication(
                            fromLogReplication(repl),
                            conf);
            fsDir.unprotectedSetReplication(src.toString(), 
                replication,
                null);
            break;
          } 
          case OP_RENAME: {
            UTF8 src = new UTF8();
            UTF8 dst = new UTF8();
            src.readFields(in);
            dst.readFields(in);
            fsDir.unprotectedRenameTo(src, dst);
            break;
          }
          case OP_DELETE: {
            UTF8 src = new UTF8();
            src.readFields(in);
            fsDir.unprotectedDelete(src);
            break;
          }
          case OP_MKDIR: {
            UTF8 src = new UTF8();
            src.readFields(in);
            fsDir.unprotectedMkdir(src.toString());
            break;
          }
          case OP_DATANODE_ADD: {
            if( logVersion > -3 )
              throw new IOException("Unexpected opcode " + opcode 
                  + " for version " + logVersion );
            DatanodeDescriptor node = new DatanodeDescriptor();
            node.readFields(in);
            fsNamesys.unprotectedAddDatanode( node );
            break;
          }
          case OP_DATANODE_REMOVE: {
            if( logVersion > -3 )
              throw new IOException("Unexpected opcode " + opcode 
                  + " for version " + logVersion );
            DatanodeID nodeID = new DatanodeID();
            nodeID.readFields(in);
            DatanodeDescriptor node = fsNamesys.getDatanode( nodeID );
            if( node != null ) {
              fsNamesys.unprotectedRemoveDatanode( node );
              // physically remove node from datanodeMap
              fsNamesys.wipeDatanode( nodeID );
            }
            break;
          }
          default: {
            throw new IOException("Never seen opcode " + opcode);
          }
          }
        }
      } finally {
        in.close();
      }
    }
    
    if( logVersion != FSConstants.DFS_CURRENT_VERSION ) // other version
      numEdits++; // save this image asap
    return numEdits;
  }
  
  static short adjustReplication( short replication, Configuration conf) {
    short minReplication = (short)conf.getInt("dfs.replication.min", 1);
    if( replication<minReplication ) {
      replication = minReplication;
    }
    short maxReplication = (short)conf.getInt("dfs.replication.max", 512);
    if( replication>maxReplication ) {
      replication = maxReplication;
    }
    return replication;
  }

  /**
   * Write an operation to the edit log
   */
  void logEdit(byte op, Writable w1, Writable w2) {
    synchronized (editsStream) {
      try {
        editsStream.write(op);
        if (w1 != null) {
          w1.write(editsStream);
        }
        if (w2 != null) {
          w2.write(editsStream);
        }
      } catch (IOException ie) {
        // TODO: Must report an error here
      }
    }
    // TODO: initialize checkpointing if the log is large enough
  }

  /** 
   * Add create file record to edit log
   */
  void logCreateFile( FSDirectory.INode newNode ) {
    UTF8 nameReplicationPair[] = new UTF8[] { 
                        new UTF8( newNode.computeName() ), 
                        FSEditLog.toLogReplication( newNode.getReplication() )};
    logEdit(OP_ADD,
            new ArrayWritable( UTF8.class, nameReplicationPair ), 
            new ArrayWritable( Block.class, newNode.getBlocks() ));
  }
  
  /** 
   * Add create directory record to edit log
   */
  void logMkDir( FSDirectory.INode newNode ) {
    logEdit(OP_MKDIR, new UTF8( newNode.computeName() ), null );
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename( UTF8 src, UTF8 dst ) {
    logEdit(OP_RENAME, src, dst);
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication( String src, short replication ) {
    logEdit(OP_SET_REPLICATION, 
            new UTF8(src), 
            FSEditLog.toLogReplication( replication ));
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete( UTF8 src ) {
    logEdit(OP_DELETE, src, null);
  }
  
  /** 
   * Creates a record in edit log corresponding to a new data node
   * registration event.
   */
  void logAddDatanode( DatanodeDescriptor node ) {
    logEdit( OP_DATANODE_ADD, node, null );
  }
  
  /** 
   * Creates a record in edit log corresponding to a data node
   * removal event.
   */
  void logRemoveDatanode( DatanodeID nodeID ) {
    logEdit( OP_DATANODE_REMOVE, new DatanodeID( nodeID ), null );
  }
  
  static UTF8 toLogReplication( short replication ) {
    return new UTF8( Short.toString(replication));
  }
  
  static short fromLogReplication( UTF8 replication ) {
    return Short.parseShort(replication.toString());
  }
}
