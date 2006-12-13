package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

interface FileSystemStore {
  
  void initialize(URI uri, Configuration conf) throws IOException;

  void storeINode(Path path, INode inode) throws IOException;
  void storeBlock(Block block, InputStream in) throws IOException;
  
  boolean inodeExists(Path path) throws IOException;
  boolean blockExists(long blockId) throws IOException;

  INode getINode(Path path) throws IOException;
  InputStream getBlockStream(Block block, long byteRangeStart) throws IOException;

  void deleteINode(Path path) throws IOException;
  void deleteBlock(Block block) throws IOException;

  Set<Path> listSubPaths(Path path) throws IOException;

  /**
   * Delete everything. Used for testing.
   * @throws IOException
   */
  void purge() throws IOException;
  
  /**
   * Diagnostic method to dump all INodes to the console.
   * @throws IOException
   */
  void dump() throws IOException;
}