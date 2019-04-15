/*
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

package org.apache.hadoop.fs.ozone;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_USER_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The minimal Ozone Filesystem implementation.
 * <p>
 * This is a basic version which doesn't extend
 * KeyProviderTokenIssuer and doesn't include statistics. It can be used
 * from older hadoop version. For newer hadoop version use the full featured
 * OzoneFileSystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BasicOzoneFileSystem extends FileSystem {
  static final Logger LOG =
      LoggerFactory.getLogger(BasicOzoneFileSystem.class);

  /**
   * The Ozone client for connecting to Ozone server.
   */

  private URI uri;
  private String userName;
  private Path workingDir;

  private OzoneClientAdapter adapter;

  private static final Pattern URL_SCHEMA_PATTERN =
      Pattern.compile("([^\\.]+)\\.([^\\.]+)\\.{0,1}(.*)");

  private static final String URI_EXCEPTION_TEXT = "Ozone file system url " +
      "should be either one of the two forms: " +
      "o3fs://bucket.volume/key  OR " +
      "o3fs://bucket.volume.om-host.example.com:5678/key";

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    Objects.requireNonNull(name.getScheme(), "No scheme provided in " + name);
    Preconditions.checkArgument(getScheme().equals(name.getScheme()),
        "Invalid scheme provided in " + name);

    String authority = name.getAuthority();

    Matcher matcher = URL_SCHEMA_PATTERN.matcher(authority);

    if (!matcher.matches()) {
      throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
    }
    String bucketStr = matcher.group(1);
    String volumeStr = matcher.group(2);
    String remaining = matcher.groupCount() == 3 ? matcher.group(3) : null;

    String omHost = null;
    String omPort = String.valueOf(-1);
    if (!isEmpty(remaining)) {
      String[] parts = remaining.split(":");
      if (parts.length != 2) {
        throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
      }
      omHost = parts[0];
      omPort = parts[1];
      if (!isNumber(omPort)) {
        throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
      }
    }

    try {
      uri = new URIBuilder().setScheme(OZONE_URI_SCHEME)
          .setHost(authority)
          .build();
      LOG.trace("Ozone URI for ozfs initialization is " + uri);

      //isolated is the default for ozonefs-lib-legacy which includes the
      // /ozonefs.txt, otherwise the default is false. It could be overridden.
      boolean defaultValue =
          BasicOzoneFileSystem.class.getClassLoader()
              .getResource("ozonefs.txt")
              != null;

      //Use string here instead of the constant as constant may not be available
      //on the classpath of a hadoop 2.7
      boolean isolatedClassloader =
          conf.getBoolean("ozone.fs.isolated-classloader", defaultValue);

      this.adapter = createAdapter(conf, bucketStr, volumeStr, omHost, omPort,
          isolatedClassloader);

      try {
        this.userName =
            UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        this.userName = OZONE_DEFAULT_USER;
      }
      this.workingDir = new Path(OZONE_USER_DIR, this.userName)
          .makeQualified(this.uri, this.workingDir);
    } catch (URISyntaxException ue) {
      final String msg = "Invalid Ozone endpoint " + name;
      LOG.error(msg, ue);
      throw new IOException(msg, ue);
    }
  }

  protected OzoneClientAdapter createAdapter(Configuration conf,
      String bucketStr,
      String volumeStr, String omHost, String omPort,
      boolean isolatedClassloader) throws IOException {

    if (isolatedClassloader) {

      return OzoneClientAdapterFactory
          .createAdapter(volumeStr, bucketStr);

    } else {

      return new BasicOzoneClientAdapterImpl(omHost,
          Integer.parseInt(omPort), conf,
          volumeStr, bucketStr);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      adapter.close();
    } finally {
      super.close();
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getScheme() {
    return OZONE_URI_SCHEME;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    incrementCounter(Statistic.INVOCATION_OPEN);
    statistics.incrementWriteOps(1);
    LOG.trace("open() path:{}", f);
    final FileStatus fileStatus = getFileStatus(f);
    final String key = pathToKey(f);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open directory " + f + " to read");
    }

    return new FSDataInputStream(
        new OzoneFSInputStream(adapter.readFile(key)));
  }

  protected void incrementCounter(Statistic statistic) {
    //don't do anyting in this default implementation.
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize,
      Progressable progress) throws IOException {
    LOG.trace("create() path:{}", f);
    incrementCounter(Statistic.INVOCATION_CREATE);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(f);
    final FileStatus status;
    try {
      status = getFileStatus(f);
      if (status.isDirectory()) {
        throw new FileAlreadyExistsException(f + " is a directory");
      } else {
        if (!overwrite) {
          // path references a file and overwrite is disabled
          throw new FileAlreadyExistsException(f + " already exists");
        }
        LOG.trace("Overwriting file {}", f);
        adapter.deleteObject(key);
      }
    } catch (FileNotFoundException ignored) {
      // this means the file is not found
    }

    // We pass null to FSDataOutputStream so it won't count writes that
    // are being buffered to a file
    return createOutputStream(key, overwrite, true);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    incrementCounter(Statistic.INVOCATION_CREATE_NON_RECURSIVE);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(path);
    final Path parent = path.getParent();
    if (parent != null) {
      // expect this to raise an exception if there is no parent
      if (!getFileStatus(parent).isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }
    return createOutputStream(key, flags.contains(CreateFlag.OVERWRITE), false);
  }

  private FSDataOutputStream createOutputStream(String key, boolean overwrite,
      boolean recursive) throws IOException {
    return new FSDataOutputStream(adapter.createFile(key, overwrite, recursive),
        statistics);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("append() Not implemented by the "
        + getClass().getSimpleName() + " FileSystem implementation");
  }

  private class RenameIterator extends OzoneListingIterator {
    private final String srcKey;
    private final String dstKey;

    RenameIterator(Path srcPath, Path dstPath)
        throws IOException {
      super(srcPath);
      srcKey = pathToKey(srcPath);
      dstKey = pathToKey(dstPath);
      LOG.trace("rename from:{} to:{}", srcKey, dstKey);
    }

    @Override
    boolean processKey(String key) throws IOException {
      String newKeyName = dstKey.concat(key.substring(srcKey.length()));
      adapter.renameKey(key, newKeyName);
      return true;
    }
  }

  /**
   * Check whether the source and destination path are valid and then perform
   * rename from source path to destination path.
   * <p>
   * The rename operation is performed by renaming the keys with src as prefix.
   * For such keys the prefix is changed from src to dst.
   *
   * @param src source path for rename
   * @param dst destination path for rename
   * @return true if rename operation succeeded or
   * if the src and dst have the same path and are of the same type
   * @throws IOException on I/O errors or if the src/dst paths are invalid.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    incrementCounter(Statistic.INVOCATION_RENAME);
    statistics.incrementWriteOps(1);
    if (src.equals(dst)) {
      return true;
    }

    LOG.trace("rename() from:{} to:{}", src, dst);
    if (src.isRoot()) {
      // Cannot rename root of file system
      LOG.trace("Cannot rename the root of a filesystem");
      return false;
    }

    // Cannot rename a directory to its own subdirectory
    Path dstParent = dst.getParent();
    while (dstParent != null && !src.equals(dstParent)) {
      dstParent = dstParent.getParent();
    }
    Preconditions.checkArgument(dstParent == null,
        "Cannot rename a directory to its own subdirectory");
    // Check if the source exists
    FileStatus srcStatus;
    try {
      srcStatus = getFileStatus(src);
    } catch (FileNotFoundException fnfe) {
      // source doesn't exist, return
      return false;
    }

    // Check if the destination exists
    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dst);
    } catch (FileNotFoundException fnde) {
      dstStatus = null;
    }

    if (dstStatus == null) {
      // If dst doesn't exist, check whether dst parent dir exists or not
      // if the parent exists, the source can still be renamed to dst path
      dstStatus = getFileStatus(dst.getParent());
      if (!dstStatus.isDirectory()) {
        throw new IOException(String.format(
            "Failed to rename %s to %s, %s is a file", src, dst,
            dst.getParent()));
      }
    } else {
      // if dst exists and source and destination are same,
      // check both the src and dst are of same type
      if (srcStatus.getPath().equals(dstStatus.getPath())) {
        return !srcStatus.isDirectory();
      } else if (dstStatus.isDirectory()) {
        // If dst is a directory, rename source as subpath of it.
        // for example rename /source to /dst will lead to /dst/source
        dst = new Path(dst, src.getName());
        FileStatus[] statuses;
        try {
          statuses = listStatus(dst);
        } catch (FileNotFoundException fnde) {
          statuses = null;
        }

        if (statuses != null && statuses.length > 0) {
          // If dst exists and not a directory not empty
          throw new FileAlreadyExistsException(String.format(
              "Failed to rename %s to %s, file already exists or not empty!",
              src, dst));
        }
      } else {
        // If dst is not a directory
        throw new FileAlreadyExistsException(String.format(
            "Failed to rename %s to %s, file already exists!", src, dst));
      }
    }

    if (srcStatus.isDirectory()) {
      if (dst.toString().startsWith(src.toString() + OZONE_URI_DELIMITER)) {
        LOG.trace("Cannot rename a directory to a subdirectory of self");
        return false;
      }
    }
    RenameIterator iterator = new RenameIterator(src, dst);
    return iterator.iterate();
  }

  private class DeleteIterator extends OzoneListingIterator {
    private boolean recursive;

    DeleteIterator(Path f, boolean recursive)
        throws IOException {
      super(f);
      this.recursive = recursive;
      if (getStatus().isDirectory()
          && !this.recursive
          && listStatus(f).length != 0) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }
    }

    @Override
    boolean processKey(String key) throws IOException {
      if (key.equals("")) {
        LOG.trace("Skipping deleting root directory");
        return true;
      } else {
        LOG.trace("deleting key:" + key);
        boolean succeed = adapter.deleteObject(key);
        // if recursive delete is requested ignore the return value of
        // deleteObject and issue deletes for other keys.
        return recursive || succeed;
      }
    }
  }

  /**
   * Deletes the children of the input dir path by iterating though the
   * DeleteIterator.
   *
   * @param f directory path to be deleted
   * @return true if successfully deletes all required keys, false otherwise
   * @throws IOException
   */
  private boolean innerDelete(Path f, boolean recursive) throws IOException {
    LOG.trace("delete() path:{} recursive:{}", f, recursive);
    try {
      DeleteIterator iterator = new DeleteIterator(f, recursive);
      return iterator.iterate();
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist", f);
      return false;
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    incrementCounter(Statistic.INVOCATION_DELETE);
    statistics.incrementWriteOps(1);
    LOG.debug("Delete path {} - recursive {}", f, recursive);
    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException ex) {
      LOG.warn("delete: Path does not exist: {}", f);
      return false;
    }

    String key = pathToKey(f);
    boolean result;

    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {}", f);
      key = addTrailingSlashIfNeeded(key);

      if (key.equals("/")) {
        LOG.warn("Cannot delete root directory.");
        return false;
      }

      result = innerDelete(f, recursive);
    } else {
      LOG.debug("delete: Path is a file: {}", f);
      result = adapter.deleteObject(key);
    }

    if (result) {
      // If this delete operation removes all files/directories from the
      // parent direcotry, then an empty parent directory must be created.
      Path parent = f.getParent();
      if (parent != null && !parent.isRoot()) {
        createFakeDirectoryIfNecessary(parent);
      }
    }

    return result;
  }

  /**
   * Create a fake parent directory key if it does not already exist and no
   * other child of this parent directory exists.
   *
   * @param f path to the fake parent directory
   * @throws IOException
   */
  private void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    if (!key.isEmpty() && !o3Exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      String dirKey = addTrailingSlashIfNeeded(key);
      adapter.createDirectory(dirKey);
    }
  }

  /**
   * Check if a file or directory exists corresponding to given path.
   *
   * @param f path to file/directory.
   * @return true if it exists, false otherwise.
   * @throws IOException
   */
  private boolean o3Exists(final Path f) throws IOException {
    Path path = makeQualified(f);
    try {
      getFileStatus(path);
      return true;
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  private class ListStatusIterator extends OzoneListingIterator {
    // _fileStatuses_ maintains a list of file(s) which is either the input
    // path itself or a child of the input directory path.
    private List<FileStatus> fileStatuses = new ArrayList<>(LISTING_PAGE_SIZE);
    // _subDirStatuses_ maintains a list of sub-dirs of the input directory
    // path.
    private Map<Path, FileStatus> subDirStatuses =
        new HashMap<>(LISTING_PAGE_SIZE);
    private Path f; // the input path

    ListStatusIterator(Path f) throws IOException {
      super(f);
      this.f = f;
    }

    /**
     * Add the key to the listStatus result if the key corresponds to the
     * input path or is an immediate child of the input path.
     *
     * @param key key to be processed
     * @return always returns true
     * @throws IOException
     */
    @Override
    boolean processKey(String key) throws IOException {
      Path keyPath = new Path(OZONE_URI_DELIMITER + key);
      if (key.equals(getPathKey())) {
        if (pathIsDirectory()) {
          // if input path is a directory, we add the sub-directories and
          // files under this directory.
          return true;
        } else {
          addFileStatus(keyPath);
          return true;
        }
      }
      // Left with only subkeys now
      // We add only the immediate child files and sub-dirs i.e. we go only
      // upto one level down the directory tree structure.
      if (pathToKey(keyPath.getParent()).equals(pathToKey(f))) {
        // This key is an immediate child. Can be file or directory
        if (key.endsWith(OZONE_URI_DELIMITER)) {
          // Key is a directory
          addSubDirStatus(keyPath);
        } else {
          addFileStatus(keyPath);
        }
      } else {
        // This key is not the immediate child of the input directory. So we
        // traverse the parent tree structure of this key until we get the
        // immediate child of the input directory.
        Path immediateChildPath = getImmediateChildPath(keyPath.getParent());
        if (immediateChildPath != null) {
          addSubDirStatus(immediateChildPath);
        }
      }
      return true;
    }

    /**
     * Adds the FileStatus of keyPath to final result of listStatus.
     *
     * @param filePath path to the file
     * @throws FileNotFoundException
     */
    void addFileStatus(Path filePath) throws IOException {
      fileStatuses.add(getFileStatus(filePath));
    }

    /**
     * Adds the FileStatus of the subdir to final result of listStatus, if not
     * already included.
     *
     * @param dirPath path to the dir
     * @throws FileNotFoundException
     */
    void addSubDirStatus(Path dirPath) throws IOException {
      // Check if subdir path is already included in statuses.
      if (!subDirStatuses.containsKey(dirPath)) {
        subDirStatuses.put(dirPath, getFileStatus(dirPath));
      }
    }

    /**
     * Traverse the parent directory structure of keyPath to determine the
     * which parent/ grand-parent/.. is the immediate child of the input path f.
     *
     * @param keyPath path whose parent directory structure should be traversed.
     * @return immediate child path of the input path f.
     */
    Path getImmediateChildPath(Path keyPath) {
      Path path = keyPath;
      Path parent = path.getParent();
      while (parent != null) {
        if (pathToKey(parent).equals(pathToKey(f))) {
          return path;
        }
        path = parent;
        parent = path.getParent();
      }
      return null;
    }

    /**
     * Return the result of listStatus operation. If the input path is a
     * file, return the status for only that file. If the input path is a
     * directory, return the statuses for all the child files and sub-dirs.
     */
    FileStatus[] getStatuses() {
      List<FileStatus> result = Stream.concat(
          fileStatuses.stream(), subDirStatuses.values().stream())
          .collect(Collectors.toList());
      return result.toArray(new FileStatus[result.size()]);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_STATUS);
    statistics.incrementReadOps(1);
    LOG.trace("listStatus() path:{}", f);
    ListStatusIterator iterator = new ListStatusIterator(f);
    iterator.iterate();
    return iterator.getStatuses();
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return adapter.getDelegationToken(renewer);
  }

  /**
   * Get a canonical service name for this file system. If the URI is logical,
   * the hostname part of the URI will be returned.
   *
   * @return a service string that uniquely identifies this file system.
   */
  @Override
  public String getCanonicalServiceName() {
    return adapter.getCanonicalServiceName();
  }

  /**
   * Get the username of the FS.
   *
   * @return the short name of the user who instantiated the FS
   */
  public String getUsername() {
    return userName;
  }

  /**
   * Check whether the path is valid and then create directories.
   * Directory is represented using a key with no value.
   * All the non-existent parent directories are also created.
   *
   * @param path directory path to be created
   * @return true if directory exists or created successfully.
   * @throws IOException
   */
  private boolean mkdir(Path path) throws IOException {
    Path fPart = path;
    Path prevfPart = null;
    do {
      LOG.trace("validating path:{}", fPart);
      try {
        FileStatus fileStatus = getFileStatus(fPart);
        if (fileStatus.isDirectory()) {
          // If path exists and a directory, exit
          break;
        } else {
          // Found a file here, rollback and delete newly created directories
          LOG.trace("Found a file with same name as directory, path:{}", fPart);
          if (prevfPart != null) {
            delete(prevfPart, true);
          }
          throw new FileAlreadyExistsException(String.format(
              "Can't make directory for path '%s', it is a file.", fPart));
        }
      } catch (FileNotFoundException fnfe) {
        LOG.trace("creating directory for fpart:{}", fPart);
        String key = pathToKey(fPart);
        String dirKey = addTrailingSlashIfNeeded(key);
        if (!adapter.createDirectory(dirKey)) {
          // Directory creation failed here,
          // rollback and delete newly created directories
          LOG.trace("Directory creation failed, path:{}", fPart);
          if (prevfPart != null) {
            delete(prevfPart, true);
          }
          return false;
        }
      }
      prevfPart = fPart;
      fPart = fPart.getParent();
    } while (fPart != null);
    return true;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    LOG.trace("mkdir() path:{} ", f);
    String key = pathToKey(f);
    if (isEmpty(key)) {
      return false;
    }
    return mkdir(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_GET_FILE_STATUS);
    statistics.incrementReadOps(1);
    LOG.trace("getFileStatus() path:{}", f);
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);

    return adapter.getFileStatus(key)
        .makeQualified(uri, qualifiedPath, getUsername(), getUsername());
  }

  /**
   * Turn a path (relative or otherwise) into an Ozone key.
   *
   * @param path the path of the file.
   * @return the key of the object that represents the file.
   */
  public String pathToKey(Path path) {
    Objects.requireNonNull(path, "Path canf not be null!");
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }
    // removing leading '/' char
    String key = path.toUri().getPath().substring(1);
    LOG.trace("path for key:{} is:{}", key, path);
    return key;
  }

  /**
   * Add trailing delimiter to path if it is already not present.
   *
   * @param key the ozone Key which needs to be appended
   * @return delimiter appended key
   */
  private String addTrailingSlashIfNeeded(String key) {
    if (!isEmpty(key) && !key.endsWith(OZONE_URI_DELIMITER)) {
      return key + OZONE_URI_DELIMITER;
    } else {
      return key;
    }
  }

  @Override
  public String toString() {
    return "OzoneFileSystem{URI=" + uri + ", "
        + "workingDir=" + workingDir + ", "
        + "userName=" + userName + ", "
        + "statistics=" + statistics
        + "}";
  }

  /**
   * This class provides an interface to iterate through all the keys in the
   * bucket prefixed with the input path key and process them.
   * <p>
   * Each implementing class should define how the keys should be processed
   * through the processKey() function.
   */
  private abstract class OzoneListingIterator {
    private final Path path;
    private final FileStatus status;
    private String pathKey;
    private Iterator<BasicKeyInfo> keyIterator;

    OzoneListingIterator(Path path)
        throws IOException {
      this.path = path;
      this.status = getFileStatus(path);
      this.pathKey = pathToKey(path);
      if (status.isDirectory()) {
        this.pathKey = addTrailingSlashIfNeeded(pathKey);
      }
      keyIterator = adapter.listKeys(pathKey);
    }

    /**
     * The output of processKey determines if further iteration through the
     * keys should be done or not.
     *
     * @return true if we should continue iteration of keys, false otherwise.
     * @throws IOException
     */
    abstract boolean processKey(String key) throws IOException;

    /**
     * Iterates thorugh all the keys prefixed with the input path's key and
     * processes the key though processKey().
     * If for any key, the processKey() returns false, then the iteration is
     * stopped and returned with false indicating that all the keys could not
     * be processed successfully.
     *
     * @return true if all keys are processed successfully, false otherwise.
     * @throws IOException
     */
    boolean iterate() throws IOException {
      LOG.trace("Iterating path {}", path);
      if (status.isDirectory()) {
        LOG.trace("Iterating directory:{}", pathKey);
        while (keyIterator.hasNext()) {
          BasicKeyInfo key = keyIterator.next();
          LOG.trace("iterating key:{}", key.getName());
          if (!processKey(key.getName())) {
            return false;
          }
        }
        return true;
      } else {
        LOG.trace("iterating file:{}", path);
        return processKey(pathKey);
      }
    }

    String getPathKey() {
      return pathKey;
    }

    boolean pathIsDirectory() {
      return status.isDirectory();
    }

    FileStatus getStatus() {
      return status;
    }
  }

  public OzoneClientAdapter getAdapter() {
    return adapter;
  }

  public boolean isEmpty(CharSequence cs) {
    return cs == null || cs.length() == 0;
  }

  public boolean isNumber(String number) {
    try {
      Integer.parseInt(number);
    } catch (NumberFormatException ex) {
      return false;
    }
    return true;
  }
}
