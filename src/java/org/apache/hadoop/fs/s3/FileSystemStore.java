package org.apache.hadoop.fs.s3;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A facility for storing and retrieving {@link INode}s and {@link Block}s.
 */
public interface FileSystemStore {
  
  void initialize(URI uri, Configuration conf) throws IOException;
  String getVersion() throws IOException;

  void storeINode(Path path, INode inode) throws IOException;
  void storeBlock(Block block, File file) throws IOException;
  
  boolean inodeExists(Path path) throws IOException;
  boolean blockExists(long blockId) throws IOException;

  INode retrieveINode(Path path) throws IOException;
  File retrieveBlock(Block block, long byteRangeStart) throws IOException;

  void deleteINode(Path path) throws IOException;
  void deleteBlock(Block block) throws IOException;

  Set<Path> listSubPaths(Path path) throws IOException;
  Set<Path> listDeepSubPaths(Path path) throws IOException;

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