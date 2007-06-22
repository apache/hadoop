package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A {@link FileSystem} backed by <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * </p>
 */
public class S3FileSystem extends FileSystem {

  private static final long DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
  
  private URI uri;

  private FileSystemStore store;

  private FileSystem localFs;

  private Path workingDir = new Path("/user", System.getProperty("user.name"));

  public S3FileSystem() {
    // set store in initialize()
  }
  
  public S3FileSystem(FileSystemStore store) {
    this.store = store;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());    
    this.localFs = get(URI.create("file:///"), conf);
  }  

  private static FileSystemStore createDefaultStore(Configuration conf) {
    FileSystemStore store = new Jets3tFileSystemStore();
    
    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                                                               conf.getInt("fs.s3.maxRetries", 4),
                                                                               conf.getLong("fs.s3.sleepTimeSeconds", 10), TimeUnit.SECONDS);
    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(IOException.class, basePolicy);
    exceptionToPolicyMap.put(S3Exception.class, basePolicy);
    
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
                                                              RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    methodNameToPolicyMap.put("storeBlock", methodPolicy);
    methodNameToPolicyMap.put("retrieveBlock", methodPolicy);
    
    return (FileSystemStore) RetryProxy.create(FileSystemStore.class,
                                               store, methodNameToPolicyMap);
  }
  
  @Override
  public String getName() {
    return getUri().toString();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  @Override
  public boolean exists(Path path) throws IOException {
    return store.inodeExists(makeAbsolute(path));
  }

  @Override
  public boolean mkdirs(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      store.storeINode(absolutePath, INode.DIRECTORY_INODE);
    } else if (inode.isFile()) {
      throw new IOException(String.format(
                                          "Can't make directory for path %s since it is a file.", absolutePath));
    }
    Path parent = absolutePath.getParent();
    return (parent == null || mkdirs(parent));
  }

  @Override
  public boolean isFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      return false;
    }
    return inode.isFile();
  }

  private INode checkFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      throw new IOException("No such file.");
    }
    if (inode.isDirectory()) {
      throw new IOException("Path " + path + " is a directory.");
    }
    return inode;
  }

  @Override
  public Path[] listPaths(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      return null;
    } else if (inode.isFile()) {
      return new Path[] { absolutePath };
    } else { // directory
      Set<Path> paths = store.listSubPaths(absolutePath);
      return paths.toArray(new Path[0]);
    }
  }

  @Override
  public FSDataOutputStream create(Path file, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress)
    throws IOException {

    INode inode = store.retrieveINode(makeAbsolute(file));
    if (inode != null) {
      if (overwrite) {
        delete(file);
      } else {
        throw new IOException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new IOException("Mkdirs failed to create " + parent.toString());
        }
      }      
    }
    return new FSDataOutputStream(
                                  new S3OutputStream(getConf(), store, makeAbsolute(file),
                                                     blockSize, progress), bufferSize);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    INode inode = checkFile(path);
    return new FSDataInputStream(new S3InputStream(getConf(), store, inode),
                                 bufferSize);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absoluteSrc = makeAbsolute(src);
    INode srcINode = store.retrieveINode(absoluteSrc);
    if (srcINode == null) {
      // src path doesn't exist
      return false; 
    }
    Path absoluteDst = makeAbsolute(dst);
    INode dstINode = store.retrieveINode(absoluteDst);
    if (dstINode != null && dstINode.isDirectory()) {
      absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
      dstINode = store.retrieveINode(absoluteDst);
    }
    if (dstINode != null) {
      // dst path already exists - can't overwrite
      return false;
    }
    Path dstParent = absoluteDst.getParent();
    if (dstParent != null) {
      INode dstParentINode = store.retrieveINode(dstParent);
      if (dstParentINode == null || dstParentINode.isFile()) {
        // dst parent doesn't exist or is a file
        return false;
      }
    }
    return renameRecursive(absoluteSrc, absoluteDst);
  }
  
  private boolean renameRecursive(Path src, Path dst) throws IOException {
    INode srcINode = store.retrieveINode(src);
    store.storeINode(dst, srcINode);
    store.deleteINode(src);
    if (srcINode.isDirectory()) {
      for (Path oldSrc : store.listDeepSubPaths(src)) {
        INode inode = store.retrieveINode(oldSrc);
        if (inode == null) {
          return false;
        }
        Path newDst = new Path(oldSrc.toString().replaceFirst(src.toString(), dst.toString()));
        store.storeINode(newDst, inode);
        store.deleteINode(oldSrc);
      }
    }
    return true;
  }

  @Override
  public boolean delete(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      return false;
    }
    if (inode.isFile()) {
      store.deleteINode(absolutePath);
      for (Block block : inode.getBlocks()) {
        store.deleteBlock(block);
      }
    } else {
      Path[] contents = listPaths(absolutePath);
      if (contents == null) {
        return false;
      }
      for (Path p : contents) {
        if (!delete(p)) {
          return false;
        }
      }
      store.deleteINode(absolutePath);
    }
    return true;
  }

  /**
   * Replication is not supported for S3 file systems since S3 handles it for
   * us.
   */
  @Override
  public short getDefaultReplication() {
    return 1;
  }

  /**
   * FileStatus for S3 file systems. 
   */
  @Override
  public FileStatus getFileStatus(Path f)  throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(f));
    if (inode == null) {
      throw new IOException(f.toString() + ": No such file or directory.");
    }
    return new S3FileStatus(inode);
  }

  /**
   * Replication is not supported for S3 file systems since S3 handles it for
   * us.
   */
  @Override
  public boolean setReplication(Path path, short replication)
    throws IOException {
    return true;
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong("fs.s3.block.size", DEFAULT_BLOCK_SIZE);
  }

  /**
   * Return 1x1 'localhost' cell if the file exists. Return null if otherwise.
   */
  @Override
  public String[][] getFileCacheHints(Path f, long start, long len)
    throws IOException {
    // TODO: Check this is the correct behavior
    if (!exists(f)) {
      return null;
    }
    return new String[][] { { "localhost" } };
  }

  /** @deprecated */ @Deprecated
    @Override
    public void lock(Path path, boolean shared) throws IOException {
    // TODO: Design and implement
  }

  /** @deprecated */ @Deprecated
    @Override
    public void release(Path path) throws IOException {
    // TODO: Design and implement
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    FileUtil.copy(localFs, src, this, dst, delSrc, getConf());
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    FileUtil.copy(this, src, localFs, dst, delSrc, getConf());
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  // diagnostic methods

  void dump() throws IOException {
    store.dump();
  }

  void purge() throws IOException {
    store.purge();
  }

  private static class S3FileStatus implements FileStatus {
    private long length = 0, blockSize = 0;
    private boolean isDir;

    S3FileStatus(INode inode) throws IOException {
      isDir = inode.isDirectory();
      if (!isDir) {
        for (Block block : inode.getBlocks()) {
          length += block.getLength();
          if (blockSize == 0) {
            blockSize = block.getLength();
          }
        }
      }
    }
    public long getLen() {
      return length;
    }
    public boolean isDir() {
      return isDir;
    }
    public long getBlockSize() {
      return blockSize;
    }
    public short getReplication() {
      return 1;
    }
    public long getModificationTime() {
      return 0;  // not supported yet
    }
  }
}
