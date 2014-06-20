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

package org.apache.hadoop.fs.azure;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemMetricsSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.windowsazure.storage.core.Utility;

/**
 * <p>
 * A {@link FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>. This implementation is
 * blob-based and stores files on Azure in their native form so they can be read
 * by other Azure tools.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NativeAzureFileSystem extends FileSystem {

  @Override
  public String getScheme() {
    return "wasb";
  }

  
  /**
   * <p>
   * A {@link FileSystem} for reading and writing files stored on <a
   * href="http://store.azure.com/">Windows Azure</a>. This implementation is
   * blob-based and stores files on Azure in their native form so they can be read
   * by other Azure tools. This implementation uses HTTPS for secure network communication.
   * </p>
   */
  public static class Secure extends NativeAzureFileSystem {
    @Override
    public String getScheme() {
      return "wasbs";
    }
  }

  public static final Log LOG = LogFactory.getLog(NativeAzureFileSystem.class);

  static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  /**
   * The time span in seconds before which we consider a temp blob to be
   * dangling (not being actively uploaded to) and up for reclamation.
   * 
   * So e.g. if this is 60, then any temporary blobs more than a minute old
   * would be considered dangling.
   */
  static final String AZURE_TEMP_EXPIRY_PROPERTY_NAME = "fs.azure.fsck.temp.expiry.seconds";
  private static final int AZURE_TEMP_EXPIRY_DEFAULT = 3600;
  static final String PATH_DELIMITER = Path.SEPARATOR;
  static final String AZURE_TEMP_FOLDER = "_$azuretmpfolder$";

  private static final int AZURE_LIST_ALL = -1;
  private static final int AZURE_UNBOUNDED_DEPTH = -1;

  private static final long MAX_AZURE_BLOCK_SIZE = 512 * 1024 * 1024L;

  /**
   * The configuration property that determines which group owns files created
   * in WASB.
   */
  private static final String AZURE_DEFAULT_GROUP_PROPERTY_NAME = "fs.azure.permissions.supergroup";
  /**
   * The default value for fs.azure.permissions.supergroup. Chosen as the same
   * default as DFS.
   */
  static final String AZURE_DEFAULT_GROUP_DEFAULT = "supergroup";

  static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME = "fs.azure.block.location.impersonatedhost";
  private static final String AZURE_BLOCK_LOCATION_HOST_DEFAULT = "localhost";

  private class NativeAzureFsInputStream extends FSInputStream {
    private InputStream in;
    private final String key;
    private long pos = 0;

    public NativeAzureFsInputStream(DataInputStream in, String key) {
      this.in = in;
      this.key = key;
    }

    /*
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an integer in the range 0 to 255. If no byte is available
     * because the end of the stream has been reached, the value -1 is returned.
     * This method blocks until input data is available, the end of the stream
     * is detected, or an exception is thrown.
     * 
     * @returns int An integer corresponding to the byte read.
     */
    @Override
    public synchronized int read() throws IOException {
      int result = 0;
      result = in.read();
      if (result != -1) {
        pos++;
        if (statistics != null) {
          statistics.incrementBytesRead(1);
        }
      }

      // Return to the caller with the result.
      //
      return result;
    }

    /*
     * Reads up to len bytes of data from the input stream into an array of
     * bytes. An attempt is made to read as many as len bytes, but a smaller
     * number may be read. The number of bytes actually read is returned as an
     * integer. This method blocks until input data is available, end of file is
     * detected, or an exception is thrown. If len is zero, then no bytes are
     * read and 0 is returned; otherwise, there is an attempt to read at least
     * one byte. If no byte is available because the stream is at end of file,
     * the value -1 is returned; otherwise, at least one byte is read and stored
     * into b.
     * 
     * @param b -- the buffer into which data is read
     * 
     * @param off -- the start offset in the array b at which data is written
     * 
     * @param len -- the maximum number of bytes read
     * 
     * @ returns int The total number of byes read into the buffer, or -1 if
     * there is no more data because the end of stream is reached.
     */
    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
      int result = 0;
      result = in.read(b, off, len);
      if (result > 0) {
        pos += result;
      }

      if (null != statistics) {
        statistics.incrementBytesRead(result);
      }

      // Return to the caller with the result.
      return result;
    }

    @Override
    public synchronized void close() throws IOException {
      in.close();
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
      in.close();
      in = store.retrieve(key, pos);
      this.pos = pos;
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }

  private class NativeAzureFsOutputStream extends OutputStream {
    // We should not override flush() to actually close current block and flush
    // to DFS, this will break applications that assume flush() is a no-op.
    // Applications are advised to use Syncable.hflush() for that purpose.
    // NativeAzureFsOutputStream needs to implement Syncable if needed.
    private String key;
    private String keyEncoded;
    private OutputStream out;

    public NativeAzureFsOutputStream(OutputStream out, String aKey,
        String anEncodedKey) throws IOException {
      // Check input arguments. The output stream should be non-null and the
      // keys
      // should be valid strings.
      if (null == out) {
        throw new IllegalArgumentException(
            "Illegal argument: the output stream is null.");
      }

      if (null == aKey || 0 == aKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the key string is null or empty");
      }

      if (null == anEncodedKey || 0 == anEncodedKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the encoded key string is null or empty");
      }

      // Initialize the member variables with the incoming parameters.
      this.out = out;

      setKey(aKey);
      setEncodedKey(anEncodedKey);
    }

    @Override
    public synchronized void close() throws IOException {
      if (out != null) {
        // Close the output stream and decode the key for the output stream
        // before returning to the caller.
        //
        out.close();
        restoreKey();
        out = null;
      }
    }

    /**
     * Writes the specified byte to this output stream. The general contract for
     * write is that one byte is written to the output stream. The byte to be
     * written is the eight low-order bits of the argument b. The 24 high-order
     * bits of b are ignored.
     * 
     * @param b
     *          32-bit integer of block of 4 bytes
     */
    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    /**
     * Writes b.length bytes from the specified byte array to this output
     * stream. The general contract for write(b) is that it should have exactly
     * the same effect as the call write(b, 0, b.length).
     * 
     * @param b
     *          Block of bytes to be written to the output stream.
     */
    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
    }

    /**
     * Writes <code>len</code> from the specified byte array starting at offset
     * <code>off</code> to the output stream. The general contract for write(b,
     * off, len) is that some of the bytes in the array <code>
     * b</code b> are written to the output stream in order; element
     * <code>b[off]</code> is the first byte written and
     * <code>b[off+len-1]</code> is the last byte written by this operation.
     * 
     * @param b
     *          Byte array to be written.
     * @param off
     *          Write this offset in stream.
     * @param len
     *          Number of bytes to be written.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getKey() {
      return key;
    }

    /**
     * Set the blob name.
     * 
     * @param key
     *          Blob name.
     */
    public void setKey(String key) {
      this.key = key;
    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getEncodedKey() {
      return keyEncoded;
    }

    /**
     * Set the blob name.
     * 
     * @param anEncodedKey
     *          Blob name.
     */
    public void setEncodedKey(String anEncodedKey) {
      this.keyEncoded = anEncodedKey;
    }

    /**
     * Restore the original key name from the m_key member variable. Note: The
     * output file stream is created with an encoded blob store key to guarantee
     * load balancing on the front end of the Azure storage partition servers.
     * The create also includes the name of the original key value which is
     * stored in the m_key member variable. This method should only be called
     * when the stream is closed.
     * 
     * @param anEncodedKey
     *          Encoding of the original key stored in m_key member.
     */
    private void restoreKey() throws IOException {
      store.rename(getEncodedKey(), getKey());
    }
  }

  private URI uri;
  private NativeFileSystemStore store;
  private AzureNativeFileSystemStore actualStore;
  private Path workingDir;
  private long blockSize = MAX_AZURE_BLOCK_SIZE;
  private AzureFileSystemInstrumentation instrumentation;
  private String metricsSourceName;
  private boolean isClosed = false;
  private static boolean suppressRetryPolicy = false;
  // A counter to create unique (within-process) names for my metrics sources.
  private static AtomicInteger metricsSourceNameCounter = new AtomicInteger();

  
  public NativeAzureFileSystem() {
    // set store in initialize()
  }

  public NativeAzureFileSystem(NativeFileSystemStore store) {
    this.store = store;
  }

  /**
   * Suppress the default retry policy for the Storage, useful in unit tests to
   * test negative cases without waiting forever.
   */
  @VisibleForTesting
  static void suppressRetryPolicy() {
    suppressRetryPolicy = true;
  }

  /**
   * Undo the effect of suppressRetryPolicy.
   */
  @VisibleForTesting
  static void resumeRetryPolicy() {
    suppressRetryPolicy = false;
  }

  /**
   * Creates a new metrics source name that's unique within this process.
   */
  @VisibleForTesting
  public static String newMetricsSourceName() {
    int number = metricsSourceNameCounter.incrementAndGet();
    final String baseName = "AzureFileSystemMetrics";
    if (number == 1) { // No need for a suffix for the first one
      return baseName;
    } else {
      return baseName + number;
    }
  }
  
  /**
   * Checks if the given URI scheme is a scheme that's affiliated with the Azure
   * File System.
   * 
   * @param scheme
   *          The URI scheme.
   * @return true iff it's an Azure File System URI scheme.
   */
  private static boolean isWasbScheme(String scheme) {
    // The valid schemes are: asv (old name), asvs (old name over HTTPS),
    // wasb (new name), wasbs (new name over HTTPS).
    return scheme != null
        && (scheme.equalsIgnoreCase("asv") || scheme.equalsIgnoreCase("asvs")
            || scheme.equalsIgnoreCase("wasb") || scheme
              .equalsIgnoreCase("wasbs"));
  }

  /**
   * Puts in the authority of the default file system if it is a WASB file
   * system and the given URI's authority is null.
   * 
   * @return The URI with reconstructed authority if necessary and possible.
   */
  private static URI reconstructAuthorityIfNeeded(URI uri, Configuration conf) {
    if (null == uri.getAuthority()) {
      // If WASB is the default file system, get the authority from there
      URI defaultUri = FileSystem.getDefaultUri(conf);
      if (defaultUri != null && isWasbScheme(defaultUri.getScheme())) {
        try {
          // Reconstruct the URI with the authority from the default URI.
          return new URI(uri.getScheme(), defaultUri.getAuthority(),
              uri.getPath(), uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
          // This should never happen.
          throw new Error("Bad URI construction", e);
        }
      }
    }
    return uri;
  }

  @Override
  protected void checkPath(Path path) {
    // Make sure to reconstruct the path's authority if needed
    super.checkPath(new Path(reconstructAuthorityIfNeeded(path.toUri(),
        getConf())));
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    // Check authority for the URI to guarantee that it is non-null.
    uri = reconstructAuthorityIfNeeded(uri, conf);
    if (null == uri.getAuthority()) {
      final String errMsg = String
          .format("Cannot initialize WASB file system, URI authority not recognized.");
      throw new IllegalArgumentException(errMsg);
    }
    super.initialize(uri, conf);

    if (store == null) {
      store = createDefaultStore(conf);
    }

    // Make sure the metrics system is available before interacting with Azure
    AzureFileSystemMetricsSystem.fileSystemStarted();
    metricsSourceName = newMetricsSourceName();
    String sourceDesc = "Azure Storage Volume File System metrics";
    instrumentation = new AzureFileSystemInstrumentation(conf);
    AzureFileSystemMetricsSystem.registerSource(metricsSourceName, sourceDesc,
        instrumentation);

    store.initialize(uri, conf, instrumentation);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user", UserGroupInformation.getCurrentUser()
        .getShortUserName()).makeQualified(getUri(), getWorkingDirectory());
    this.blockSize = conf.getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME,
        MAX_AZURE_BLOCK_SIZE);

    if (LOG.isDebugEnabled()) {
      LOG.debug("NativeAzureFileSystem. Initializing.");
      LOG.debug("  blockSize  = "
          + conf.getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME, MAX_AZURE_BLOCK_SIZE));
    }
  }

  private NativeFileSystemStore createDefaultStore(Configuration conf) {
    actualStore = new AzureNativeFileSystemStore();

    if (suppressRetryPolicy) {
      actualStore.suppressRetryPolicy();
    }
    return actualStore;
  }

  // Note: The logic for this method is confusing as to whether it strips the
  // last slash or not (it adds it in the beginning, then strips it at the end).
  // We should revisit that.
  private String pathToKey(Path path) {
    // Convert the path to a URI to parse the scheme, the authority, and the
    // path from the path object.
    URI tmpUri = path.toUri();
    String pathUri = tmpUri.getPath();

    // The scheme and authority is valid. If the path does not exist add a "/"
    // separator to list the root of the container.
    Path newPath = path;
    if ("".equals(pathUri)) {
      newPath = new Path(tmpUri.toString() + Path.SEPARATOR);
    }

    // Verify path is absolute if the path refers to a windows drive scheme.
    if (!newPath.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }

    String key = null;
    key = newPath.toUri().getPath();
    if (key.length() == 1) {
      return key;
    } else {
      return key.substring(1); // remove initial slash
    }
  }

  private static Path keyToPath(String key) {
    if (key.equals("/")) {
      return new Path("/"); // container
    }
    return new Path("/" + key);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /**
   * For unit test purposes, retrieves the AzureNativeFileSystemStore store
   * backing this file system.
   * 
   * @return The store object.
   */
  @VisibleForTesting
  public AzureNativeFileSystemStore getStore() {
    return actualStore;
  }
  
  /**
   * Gets the metrics source for this file system.
   * This is mainly here for unit testing purposes.
   *
   * @return the metrics source.
   */
  public AzureFileSystemInstrumentation getInstrumentation() {
    return instrumentation;
  }

  /** This optional operation is not yet supported. */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating file: " + f.toString());
    }

    if (containsColon(f)) {
      throw new IOException("Cannot create file " + f
          + " through WASB that has colons in the name");
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    FileMetadata existingMetadata = store.retrieveMetadata(key);
    if (existingMetadata != null) {
      if (existingMetadata.isDir()) {
        throw new IOException("Cannot create file " + f
            + "; already exists as a directory.");
      }
      if (!overwrite) {
        throw new IOException("File already exists:" + f);
      }
    }

    Path parentFolder = absolutePath.getParent();
    if (parentFolder != null && parentFolder.getParent() != null) { // skip root
      // Update the parent folder last modified time if the parent folder
      // already exists.
      String parentKey = pathToKey(parentFolder);
      FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
      if (parentMetadata != null
          && parentMetadata.isDir()
          && parentMetadata.getBlobMaterialization() == BlobMaterialization.Explicit) {
        store.updateFolderLastModifiedTime(parentKey);
      } else {
        // Make sure that the parent folder exists.
        mkdirs(parentFolder, permission);
      }
    }

    // Open the output blob stream based on the encoded key.
    String keyEncoded = encodeKey(key);

    // Mask the permission first (with the default permission mask as well).
    FsPermission masked = applyUMask(permission, UMaskApplyMode.NewFile);
    PermissionStatus permissionStatus = createPermissionStatus(masked);

    // First create a blob at the real key, pointing back to the temporary file
    // This accomplishes a few things:
    // 1. Makes sure we can create a file there.
    // 2. Makes it visible to other concurrent threads/processes/nodes what
    // we're
    // doing.
    // 3. Makes it easier to restore/cleanup data in the event of us crashing.
    store.storeEmptyLinkFile(key, keyEncoded, permissionStatus);

    // The key is encoded to point to a common container at the storage server.
    // This reduces the number of splits on the server side when load balancing.
    // Ingress to Azure storage can take advantage of earlier splits. We remove
    // the root path to the key and prefix a random GUID to the tail (or leaf
    // filename) of the key. Keys are thus broadly and randomly distributed over
    // a single container to ease load balancing on the storage server. When the
    // blob is committed it is renamed to its earlier key. Uncommitted blocks
    // are not cleaned up and we leave it to Azure storage to garbage collect
    // these
    // blocks.
    OutputStream bufOutStream = new NativeAzureFsOutputStream(store.storefile(
        keyEncoded, permissionStatus), key, keyEncoded);

    // Construct the data output stream from the buffered output stream.
    FSDataOutputStream fsOut = new FSDataOutputStream(bufOutStream, statistics);

    
    // Increment the counter
    instrumentation.fileCreated();
    
    // Return data output stream to caller.
    return fsOut;
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting file: " + f.toString());
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    // Capture the metadata for the path.
    //
    FileMetadata metaFile = store.retrieveMetadata(key);

    if (null == metaFile) {
      // The path to be deleted does not exist.
      return false;
    }

    // The path exists, determine if it is a folder containing objects,
    // an empty folder, or a simple file and take the appropriate actions.
    if (!metaFile.isDir()) {
      // The path specifies a file. We need to check the parent path
      // to make sure it's a proper materialized directory before we
      // delete the file. Otherwise we may get into a situation where
      // the file we were deleting was the last one in an implicit directory
      // (e.g. the blob store only contains the blob a/b and there's no
      // corresponding directory blob a) and that would implicitly delete
      // the directory as well, which is not correct.
      Path parentPath = absolutePath.getParent();
      if (parentPath.getParent() != null) {// Not root
        String parentKey = pathToKey(parentPath);
        FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
        if (!parentMetadata.isDir()) {
          // Invalid state: the parent path is actually a file. Throw.
          throw new AzureException("File " + f + " has a parent directory "
              + parentPath + " which is also a file. Can't resolve.");
        }
        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found an implicit parent directory while trying to"
                + " delete the file " + f + ". Creating the directory blob for"
                + " it in " + parentKey + ".");
          }
          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        } else {
          store.updateFolderLastModifiedTime(parentKey);
        }
      }
      instrumentation.fileDeleted();
      store.delete(key);
    } else {
      // The path specifies a folder. Recursively delete all entries under the
      // folder.
      Path parentPath = absolutePath.getParent();
      if (parentPath.getParent() != null) {
        String parentKey = pathToKey(parentPath);
        FileMetadata parentMetadata = store.retrieveMetadata(parentKey);

        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found an implicit parent directory while trying to"
                + " delete the directory " + f
                + ". Creating the directory blob for" + " it in " + parentKey
                + ".");
          }
          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        }
      }

      // List all the blobs in the current folder.
      String priorLastKey = null;
      PartialListing listing = store.listAll(key, AZURE_LIST_ALL, 1,
          priorLastKey);
      FileMetadata[] contents = listing.getFiles();
      if (!recursive && contents.length > 0) {
        // The folder is non-empty and recursive delete was not specified.
        // Throw an exception indicating that a non-recursive delete was
        // specified for a non-empty folder.
        throw new IOException("Non-recursive delete of non-empty directory "
            + f.toString());
      }

      // Delete all the files in the folder.
      for (FileMetadata p : contents) {
        // Tag on the directory name found as the suffix of the suffix of the
        // parent directory to get the new absolute path.
        String suffix = p.getKey().substring(
            p.getKey().lastIndexOf(PATH_DELIMITER));
        if (!p.isDir()) {
          store.delete(key + suffix);
          instrumentation.fileDeleted();
        } else {
          // Recursively delete contents of the sub-folders. Notice this also
          // deletes the blob for the directory.
          if (!delete(new Path(f.toString() + suffix), true)) {
            return false;
          }
        }
      }
      store.delete(key);

      // Update parent directory last modified time
      Path parent = absolutePath.getParent();
      if (parent != null && parent.getParent() != null) { // not root
        String parentKey = pathToKey(parent);
        store.updateFolderLastModifiedTime(parentKey);
      }
      instrumentation.directoryDeleted();
    }

    // File or directory was successfully deleted.
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting the file status for " + f.toString());
    }

    // Capture the absolute path and the path to key.
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (key.length() == 0) { // root always exists
      return newDirectory(null, absolutePath);
    }

    // The path is either a folder or a file. Retrieve metadata to
    // determine if it is a directory or file.
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
      if (meta.isDir()) {
        // The path is a folder with files in it.
        //
        if (LOG.isDebugEnabled()) {
          LOG.debug("Path " + f.toString() + "is a folder.");
        }

        // Return reference to the directory object.
        return newDirectory(meta, absolutePath);
      }

      // The path is a file.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found the path: " + f.toString() + " as a file.");
      }

      // Return with reference to a file object.
      return newFile(meta, absolutePath);
    }

    // File not found. Throw exception no such file or directory.
    // Note: Should never get to this point since the root always exists.
    throw new FileNotFoundException(absolutePath
        + ": No such file or directory.");
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Retrieve the status of a given path if it is a file, or of all the
   * contained files if it is a directory.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Listing status for " + f.toString());
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    Set<FileStatus> status = new TreeSet<FileStatus>();
    FileMetadata meta = store.retrieveMetadata(key);

    if (meta != null) {
      if (!meta.isDir()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found path as a file");
        }
        return new FileStatus[] { newFile(meta, absolutePath) };
      }
      String partialKey = null;
      PartialListing listing = store.list(key, AZURE_LIST_ALL, 1, partialKey);
      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subpath = keyToPath(fileMetadata.getKey());

        // Test whether the metadata represents a file or directory and
        // add the appropriate metadata object.
        //
        // Note: There was a very old bug here where directories were added
        // to the status set as files flattening out recursive listings
        // using "-lsr" down the file system hierarchy.
        if (fileMetadata.isDir()) {
          // Make sure we hide the temp upload folder
          if (fileMetadata.getKey().equals(AZURE_TEMP_FOLDER)) {
            // Don't expose that.
            continue;
          }
          status.add(newDirectory(fileMetadata, subpath));
        } else {
          status.add(newFile(fileMetadata, subpath));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found path as a directory with " + status.size()
            + " files in it.");
      }
    } else {
      // There is no metadata found for the path.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Did not find any metadata for path: " + key);
      }

      throw new FileNotFoundException("File" + f + " does not exist.");
    }

    return status.toArray(new FileStatus[0]);
  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus(meta.getLength(), false, 1, blockSize,
        meta.getLastModified(), 0, meta.getPermissionStatus().getPermission(),
        meta.getPermissionStatus().getUserName(), meta.getPermissionStatus()
            .getGroupName(),
        path.makeQualified(getUri(), getWorkingDirectory()));
  }

  private FileStatus newDirectory(FileMetadata meta, Path path) {
    return new FileStatus(0, true, 1, blockSize, meta == null ? 0
        : meta.getLastModified(), 0, meta == null ? FsPermission.getDefault()
        : meta.getPermissionStatus().getPermission(), meta == null ? "" : meta
        .getPermissionStatus().getUserName(), meta == null ? "" : meta
        .getPermissionStatus().getGroupName(), path.makeQualified(getUri(),
        getWorkingDirectory()));
  }

  private static enum UMaskApplyMode {
    NewFile, NewDirectory, ChangeExistingFile, ChangeExistingDirectory,
  }

  /**
   * Applies the applicable UMASK's on the given permission.
   * 
   * @param permission
   *          The permission to mask.
   * @param applyDefaultUmask
   *          Whether to also apply the default umask.
   * @return The masked persmission.
   */
  private FsPermission applyUMask(final FsPermission permission,
      final UMaskApplyMode applyMode) {
    FsPermission newPermission = new FsPermission(permission);
    // Apply the default umask - this applies for new files or directories.
    if (applyMode == UMaskApplyMode.NewFile
        || applyMode == UMaskApplyMode.NewDirectory) {
      newPermission = newPermission
          .applyUMask(FsPermission.getUMask(getConf()));
    }
    return newPermission;
  }

  /**
   * Creates the PermissionStatus object to use for the given permission, based
   * on the current user in context.
   * 
   * @param permission
   *          The permission for the file.
   * @return The permission status object to use.
   * @throws IOException
   *           If login fails in getCurrentUser
   */
  private PermissionStatus createPermissionStatus(FsPermission permission)
      throws IOException {
    // Create the permission status for this file based on current user
    return new PermissionStatus(UserGroupInformation.getCurrentUser()
        .getShortUserName(), getConf().get(AZURE_DEFAULT_GROUP_PROPERTY_NAME,
        AZURE_DEFAULT_GROUP_DEFAULT), permission);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating directory: " + f.toString());
    }

    if (containsColon(f)) {
      throw new IOException("Cannot create directory " + f
          + " through WASB that has colons in the name");
    }

    Path absolutePath = makeAbsolute(f);
    PermissionStatus permissionStatus = createPermissionStatus(applyUMask(
        permission, UMaskApplyMode.NewDirectory));

    ArrayList<String> keysToCreateAsFolder = new ArrayList<String>();
    ArrayList<String> keysToUpdateAsFolder = new ArrayList<String>();
    boolean childCreated = false;
    // Check that there is no file in the parent chain of the given path.
    // Stop when you get to the root
    for (Path current = absolutePath, parent = current.getParent(); parent != null; current = parent, parent = current
        .getParent()) {
      String currentKey = pathToKey(current);
      FileMetadata currentMetadata = store.retrieveMetadata(currentKey);
      if (currentMetadata != null && !currentMetadata.isDir()) {
        throw new IOException("Cannot create directory " + f + " because "
            + current + " is an existing file.");
      } else if (currentMetadata == null
          || (currentMetadata.isDir() && currentMetadata
              .getBlobMaterialization() == BlobMaterialization.Implicit)) {
        keysToCreateAsFolder.add(currentKey);
        childCreated = true;
      } else {
        // The directory already exists. Its last modified time need to be
        // updated if there is a child directory created under it.
        if (childCreated) {
          keysToUpdateAsFolder.add(currentKey);
        }
        childCreated = false;
      }
    }

    for (String currentKey : keysToCreateAsFolder) {
      store.storeEmptyFolder(currentKey, permissionStatus);
    }

    // Take the time after finishing mkdirs as the modified time, and update all
    // the existing directories' modified time to it uniformly.
    final Calendar lastModifiedCalendar = Calendar
        .getInstance(Utility.LOCALE_US);
    lastModifiedCalendar.setTimeZone(Utility.UTC_ZONE);
    Date lastModified = lastModifiedCalendar.getTime();
    for (String key : keysToUpdateAsFolder) {
      store.updateFolderLastModifiedTime(key, lastModified);
    }

    instrumentation.directoryCreated();
    
    // otherwise throws exception
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening file: " + f.toString());
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta == null) {
      throw new FileNotFoundException(f.toString());
    }
    if (meta.isDir()) {
      throw new FileNotFoundException(f.toString()
          + " is a directory not a file.");
    }

    return new FSDataInputStream(new BufferedFSInputStream(
        new NativeAzureFsInputStream(store.retrieve(key), key), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving " + src + " to " + dst);
    }

    if (containsColon(dst)) {
      throw new IOException("Cannot rename to file " + dst
          + " through WASB that has colons in the name");
    }

    String srcKey = pathToKey(makeAbsolute(src));

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      return false;
    }

    FileMetadata srcMetadata = store.retrieveMetadata(srcKey);
    if (srcMetadata == null) {
      // Source doesn't exist
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source " + src + " doesn't exist, failing the rename.");
      }
      return false;
    }

    // Figure out the final destination
    Path absoluteDst = makeAbsolute(dst);
    String dstKey = pathToKey(absoluteDst);
    FileMetadata dstMetadata = store.retrieveMetadata(dstKey);

    // directory rename validations
    if (srcMetadata.isDir()) {

      // rename dir to self is an error
      if (srcKey.equals(dstKey)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Renaming directory to itself is disallowed. path=" + src);
        }
        return false;
      }

      // rename dir to (sub-)child of self is an error. see
      // FileSystemContractBaseTest.testRenameChildDirForbidden
      if (dstKey.startsWith(srcKey + PATH_DELIMITER)) {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Renaming directory to a itself is disallowed. src=" + src
              + " dest=" + dst);
        }
        return false;
      }
    }

    // file rename early checks
    if (!srcMetadata.isDir()) {
      if (srcKey.equals(dstKey)) {
        // rename file to self is OK
        if (LOG.isDebugEnabled()) {
          LOG.debug("Renaming file to itself. This is allowed and is treated as no-op. path="
              + src);
        }
        return true;
      }
    }

    // More validations..
    // If target is dir but target already exists, alter the dst to be a
    // subfolder.
    // eg move("/a/file.txt", "/b") where "/b" already exists causes the target
    // to be "/c/file.txt
    if (dstMetadata != null && dstMetadata.isDir()) {
      dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      // Best would be to update dstMetadata, but it is not used further, so set
      // it to null and skip the additional cost
      dstMetadata = null;
      // dstMetadata = store.retrieveMetadata(dstKey);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Destination " + dst
            + " is a directory, adjusted the destination to be " + dstKey);
      }

      // rename dir to self is an error
      if (srcKey.equals(dstKey)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Renaming directory to itself is disallowed. path=" + src);
        }
        return false;
      }

    } else if (dstMetadata != null) {
      // Otherwise, attempting to overwrite a file is error
      if (LOG.isDebugEnabled()) {
        LOG.debug("Destination " + dst
            + " is an already existing file, failing the rename.");
      }
      return false;
    } else {
      // Either dir or file and target doesn't exist.. Check that the parent
      // directory exists.
      FileMetadata parentOfDestMetadata = store
          .retrieveMetadata(pathToKey(absoluteDst.getParent()));
      if (parentOfDestMetadata == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Parent of the destination " + dst
              + " doesn't exist, failing the rename.");
        }
        return false;
      } else if (!parentOfDestMetadata.isDir()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Parent of the destination " + dst
              + " is a file, failing the rename.");
        }
        return false;
      }
    }

    // Validations complete, do the move.
    if (!srcMetadata.isDir()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source " + src + " found as a file, renaming.");
      }
      store.rename(srcKey, dstKey);
    } else {
      // Move everything inside the folder.
      String priorLastKey = null;

      // Calculate the index of the part of the string to be moved. That
      // is everything on the path up to the folder name.
      do {
        // List all blobs rooted at the source folder.
        PartialListing listing = store.listAll(srcKey, AZURE_LIST_ALL,
            AZURE_UNBOUNDED_DEPTH, priorLastKey);

        // Rename all the files in the folder.
        for (FileMetadata file : listing.getFiles()) {
          // Rename all materialized entries under the folder to point to the
          // final destination.
          if (file.getBlobMaterialization() == BlobMaterialization.Explicit) {
            String srcName = file.getKey();
            String suffix = srcName.substring(srcKey.length());
            String dstName = dstKey + suffix;
            store.rename(srcName, dstName);
          }
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);
      // Rename the top level empty blob for the folder.
      if (srcMetadata.getBlobMaterialization() == BlobMaterialization.Explicit) {
        store.rename(srcKey, dstKey);
      }
    }

    // Update both source and destination parent folder last modified time.
    Path srcParent = makeAbsolute(keyToPath(srcKey)).getParent();
    if (srcParent != null && srcParent.getParent() != null) { // not root
      String srcParentKey = pathToKey(srcParent);

      // ensure the srcParent is a materialized folder
      FileMetadata srcParentMetadata = store.retrieveMetadata(srcParentKey);
      if (srcParentMetadata.isDir()
          && srcParentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
        store.storeEmptyFolder(srcParentKey,
            createPermissionStatus(FsPermission.getDefault()));
      }

      store.updateFolderLastModifiedTime(srcParentKey);
    }

    Path destParent = makeAbsolute(keyToPath(dstKey)).getParent();
    if (destParent != null && destParent.getParent() != null) { // not root
      String dstParentKey = pathToKey(destParent);

      // ensure the dstParent is a materialized folder
      FileMetadata dstParentMetadata = store.retrieveMetadata(dstParentKey);
      if (dstParentMetadata.isDir()
          && dstParentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
        store.storeEmptyFolder(dstParentKey,
            createPermissionStatus(FsPermission.getDefault()));
      }

      store.updateFolderLastModifiedTime(dstParentKey);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Renamed " + src + " to " + dst + " successfully.");
    }
    return true;
  }

  /**
   * Return an array containing hostnames, offset and size of portions of the
   * given file. For WASB we'll just lie and give fake hosts to make sure we get
   * many splits in MR jobs.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) {
      return null;
    }

    if ((start < 0) || (len < 0)) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];
    }
    final String blobLocationHost = getConf().get(
        AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME,
        AZURE_BLOCK_LOCATION_HOST_DEFAULT);
    final String[] name = { blobLocationHost };
    final String[] host = { blobLocationHost };
    long blockSize = file.getBlockSize();
    if (blockSize <= 0) {
      throw new IllegalArgumentException(
          "The block size for the given file is not a positive number: "
              + blockSize);
    }
    int numberOfLocations = (int) (len / blockSize)
        + ((len % blockSize == 0) ? 0 : 1);
    BlockLocation[] locations = new BlockLocation[numberOfLocations];
    for (int i = 0; i < locations.length; i++) {
      long currentOffset = start + (i * blockSize);
      long currentLength = Math.min(blockSize, start + len - currentOffset);
      locations[i] = new BlockLocation(name, host, currentOffset, currentLength);
    }
    return locations;
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    Path absolutePath = makeAbsolute(p);
    String key = pathToKey(absolutePath);
    FileMetadata metadata = store.retrieveMetadata(key);
    if (metadata == null) {
      throw new FileNotFoundException("File doesn't exist: " + p);
    }
    permission = applyUMask(permission,
        metadata.isDir() ? UMaskApplyMode.ChangeExistingDirectory
            : UMaskApplyMode.ChangeExistingFile);
    if (metadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
      // It's an implicit folder, need to materialize it.
      store.storeEmptyFolder(key, createPermissionStatus(permission));
    } else if (!metadata.getPermissionStatus().getPermission()
        .equals(permission)) {
      store.changePermissionStatus(key, new PermissionStatus(metadata
          .getPermissionStatus().getUserName(), metadata.getPermissionStatus()
          .getGroupName(), permission));
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    Path absolutePath = makeAbsolute(p);
    String key = pathToKey(absolutePath);
    FileMetadata metadata = store.retrieveMetadata(key);
    if (metadata == null) {
      throw new FileNotFoundException("File doesn't exist: " + p);
    }
    PermissionStatus newPermissionStatus = new PermissionStatus(
        username == null ? metadata.getPermissionStatus().getUserName()
            : username, groupname == null ? metadata.getPermissionStatus()
            .getGroupName() : groupname, metadata.getPermissionStatus()
            .getPermission());
    if (metadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
      // It's an implicit folder, need to materialize it.
      store.storeEmptyFolder(key, newPermissionStatus);
    } else {
      store.changePermissionStatus(key, newPermissionStatus);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (isClosed) {
      return;
    }
    
    // Call the base close() to close any resources there.
    super.close();
    // Close the store
    store.close();
    
    // Notify the metrics system that this file system is closed, which may
    // trigger one final metrics push to get the accurate final file system
    // metrics out.

    long startTime = System.currentTimeMillis();

    AzureFileSystemMetricsSystem.unregisterSource(metricsSourceName);
    AzureFileSystemMetricsSystem.fileSystemClosed();

    if (LOG.isDebugEnabled()) {
        LOG.debug("Submitting metrics when file system closed took "
                + (System.currentTimeMillis() - startTime) + " ms.");
    }
    isClosed = true;
  }

  /**
   * A handler that defines what to do with blobs whose upload was interrupted.
   */
  private abstract class DanglingFileHandler {
    abstract void handleFile(FileMetadata file, FileMetadata tempFile)
        throws IOException;
  }

  /**
   * Handler implementation for just deleting dangling files and cleaning them
   * up.
   */
  private class DanglingFileDeleter extends DanglingFileHandler {
    @Override
    void handleFile(FileMetadata file, FileMetadata tempFile)
        throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting dangling file " + file.getKey());
      }
      store.delete(file.getKey());
      store.delete(tempFile.getKey());
    }
  }

  /**
   * Handler implementation for just moving dangling files to recovery location
   * (/lost+found).
   */
  private class DanglingFileRecoverer extends DanglingFileHandler {
    private final Path destination;

    DanglingFileRecoverer(Path destination) {
      this.destination = destination;
    }

    @Override
    void handleFile(FileMetadata file, FileMetadata tempFile)
        throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Recovering " + file.getKey());
      }
      // Move to the final destination
      String finalDestinationKey = pathToKey(new Path(destination,
          file.getKey()));
      store.rename(tempFile.getKey(), finalDestinationKey);
      if (!finalDestinationKey.equals(file.getKey())) {
        // Delete the empty link file now that we've restored it.
        store.delete(file.getKey());
      }
    }
  }

  /**
   * Check if a path has colons in its name
   */
  private boolean containsColon(Path p) {
    return p.toUri().getPath().toString().contains(":");
  }

  /**
   * Implements recover and delete (-move and -delete) behaviors for handling
   * dangling files (blobs whose upload was interrupted).
   * 
   * @param root
   *          The root path to check from.
   * @param handler
   *          The handler that deals with dangling files.
   */
  private void handleFilesWithDanglingTempData(Path root,
      DanglingFileHandler handler) throws IOException {
    // Calculate the cut-off for when to consider a blob to be dangling.
    long cutoffForDangling = new Date().getTime()
        - getConf().getInt(AZURE_TEMP_EXPIRY_PROPERTY_NAME,
            AZURE_TEMP_EXPIRY_DEFAULT) * 1000;
    // Go over all the blobs under the given root and look for blobs to
    // recover.
    String priorLastKey = null;
    do {
      PartialListing listing = store.listAll(pathToKey(root), AZURE_LIST_ALL,
          AZURE_UNBOUNDED_DEPTH, priorLastKey);

      for (FileMetadata file : listing.getFiles()) {
        if (!file.isDir()) { // We don't recover directory blobs
          // See if this blob has a link in it (meaning it's a place-holder
          // blob for when the upload to the temp blob is complete).
          String link = store.getLinkInFileMetadata(file.getKey());
          if (link != null) {
            // It has a link, see if the temp blob it is pointing to is
            // existent and old enough to be considered dangling.
            FileMetadata linkMetadata = store.retrieveMetadata(link);
            if (linkMetadata != null
                && linkMetadata.getLastModified() >= cutoffForDangling) {
              // Found one!
              handler.handleFile(file, linkMetadata);
            }
          }
        }
      }
      priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);
  }

  /**
   * Looks under the given root path for any blob that are left "dangling",
   * meaning that they are place-holder blobs that we created while we upload
   * the data to a temporary blob, but for some reason we crashed in the middle
   * of the upload and left them there. If any are found, we move them to the
   * destination given.
   * 
   * @param root
   *          The root path to consider.
   * @param destination
   *          The destination path to move any recovered files to.
   * @throws IOException
   */
  public void recoverFilesWithDanglingTempData(Path root, Path destination)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Recovering files with dangling temp data in " + root);
    }
    handleFilesWithDanglingTempData(root,
        new DanglingFileRecoverer(destination));
  }

  /**
   * Looks under the given root path for any blob that are left "dangling",
   * meaning that they are place-holder blobs that we created while we upload
   * the data to a temporary blob, but for some reason we crashed in the middle
   * of the upload and left them there. If any are found, we delete them.
   * 
   * @param root
   *          The root path to consider.
   * @throws IOException
   */
  public void deleteFilesWithDanglingTempData(Path root) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting files with dangling temp data in " + root);
    }
    handleFilesWithDanglingTempData(root, new DanglingFileDeleter());
  }

  @Override
  protected void finalize() throws Throwable {
    LOG.debug("finalize() called.");
    close();
    super.finalize();
  }

  /**
   * Encode the key with a random prefix for load balancing in Azure storage.
   * Upload data to a random temporary file then do storage side renaming to
   * recover the original key.
   * 
   * @param aKey
   * @param numBuckets
   * @return Encoded version of the original key.
   */
  private static String encodeKey(String aKey) {
    // Get the tail end of the key name.
    //
    String fileName = aKey.substring(aKey.lastIndexOf(Path.SEPARATOR) + 1,
        aKey.length());

    // Construct the randomized prefix of the file name. The prefix ensures the
    // file always drops into the same folder but with a varying tail key name.
    String filePrefix = AZURE_TEMP_FOLDER + Path.SEPARATOR
        + UUID.randomUUID().toString();

    // Concatenate the randomized prefix with the tail of the key name.
    String randomizedKey = filePrefix + fileName;

    // Return to the caller with the randomized key.
    return randomizedKey;
  }
}
