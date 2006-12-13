package org.apache.hadoop.fs.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;

/**
 * A stub implementation of {@link FileSystemStore} for testing
 * {@link S3FileSystem} without actually connecting to S3.
 * @author Tom White
 */
class InMemoryFileSystemStore implements FileSystemStore {
  
  private SortedMap<Path, INode> inodes = new TreeMap<Path, INode>();
  private Map<Long, byte[]> blocks = new HashMap<Long, byte[]>();
  
  public void initialize(URI uri, Configuration conf) {
    // Nothing to initialize
  }

  public void deleteINode(Path path) throws IOException {
    inodes.remove(path);
  }

  public void deleteBlock(Block block) throws IOException {
    blocks.remove(block.getId());
  }

  public boolean inodeExists(Path path) throws IOException {
    return inodes.containsKey(path);
  }

  public boolean blockExists(long blockId) throws IOException {
    return blocks.containsKey(blockId);
  }

  public INode getINode(Path path) throws IOException {
    return inodes.get(path);
  }

  public InputStream getBlockStream(Block block, long byteRangeStart) throws IOException {
    byte[] data = blocks.get(block.getId());
    return new ByteArrayInputStream(data, (int) byteRangeStart, data.length - (int) byteRangeStart);
  }

  public Set<Path> listSubPaths(Path path) throws IOException {
    // This is inefficient but more than adequate for testing purposes.
    Set<Path> subPaths = new LinkedHashSet<Path>();
    for (Path p : inodes.tailMap(path).keySet()) {
      if (path.equals(p.getParent())) {
        subPaths.add(p);
      }
    }
    return subPaths;
  }

  public void storeINode(Path path, INode inode) throws IOException {
    inodes.put(path, inode);
  }

  public void storeBlock(Block block, InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int numRead;
    while ((numRead = in.read(buf)) >= 0) {
      out.write(buf, 0, numRead);
    }
    blocks.put(block.getId(), out.toByteArray());
  }

  public void purge() throws IOException {
    inodes.clear();
    blocks.clear();
  }

  public void dump() throws IOException {
    StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append(", \n");
    for (Map.Entry<Path, INode> entry : inodes.entrySet()) {
      sb.append(entry.getKey()).append("\n");
      INode inode = entry.getValue();
      sb.append("\t").append(inode.getFileType()).append("\n");
      if (inode.getFileType() == FileType.DIRECTORY) {
        continue;
      }
      for (int j = 0; j < inode.getBlocks().length; j++) {
        sb.append("\t").append(inode.getBlocks()[j]).append("\n");
      }      
    }
    System.out.println(sb);
    
    System.out.println(inodes.keySet());
    System.out.println(blocks.keySet());
  }

}
