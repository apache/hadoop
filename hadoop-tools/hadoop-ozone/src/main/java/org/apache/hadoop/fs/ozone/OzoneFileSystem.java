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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.ozone.web.client.OzoneKey;
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ozone.web.client.OzoneBucket;
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_URI_SCHEME;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_USER_DIR;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_HTTP_SCHEME;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_URI_DELIMITER;
import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.fs.ozone.Constants.BUFFER_DIR_KEY;

/**
 * The Ozone Filesystem implementation.
 *
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(Configuration)} and variants to create
 * one. If cast to {@link OzoneFileSystem}, extra methods and features may be
 * accessed. Consider those private and unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneFileSystem extends FileSystem {
  static final Logger LOG = LoggerFactory.getLogger(OzoneFileSystem.class);

  /** The Ozone client for connecting to Ozone server. */
  private OzoneRestClient ozone;
  private OzoneBucket bucket;
  private URI uri;
  private String userName;
  private Path workingDir;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    Objects.requireNonNull(name.getScheme(), "No scheme provided in " + name);
    assert getScheme().equals(name.getScheme());

    Path path = new Path(name.getPath());
    String hostStr = name.getAuthority();
    String volumeStr = null;
    String bucketStr = null;

    while (path != null && !path.isRoot()) {
      bucketStr = volumeStr;
      volumeStr = path.getName();
      path = path.getParent();
    }

    if (hostStr == null) {
      throw new IllegalArgumentException("No host provided in " + name);
    } else if (volumeStr == null) {
      throw new IllegalArgumentException("No volume provided in " + name);
    } else if (bucketStr == null) {
      throw new IllegalArgumentException("No bucket provided in " + name);
    }

    try {
      uri = new URIBuilder().setScheme(OZONE_URI_SCHEME).setHost(hostStr)
          .setPath(OZONE_URI_DELIMITER + volumeStr + OZONE_URI_DELIMITER
              + bucketStr + OZONE_URI_DELIMITER).build();
      LOG.trace("Ozone URI for ozfs initialization is " + uri);
      this.ozone = new OzoneRestClient(OZONE_HTTP_SCHEME + hostStr);
      try {
        this.userName =
            UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        this.userName = OZONE_DEFAULT_USER;
      }
      this.ozone.setUserAuth(userName);

      OzoneVolume volume = ozone.getVolume(volumeStr);
      this.bucket = volume.getBucket(bucketStr);
      this.workingDir = new Path(OZONE_USER_DIR, this.userName)
              .makeQualified(this.uri, this.workingDir);
    } catch (OzoneException oe) {
      final String msg = "Ozone server exception when initializing file system";
      LOG.error(msg, oe);
      throw new IOException(msg, oe);
    } catch (URISyntaxException ue) {
      final String msg = "Invalid Ozone endpoint " + name;
      LOG.error(msg, ue);
      throw new IOException(msg, ue);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      ozone.close();
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
    LOG.trace("open() path:{}", f);
    final FileStatus fileStatus = getFileStatus(f);

    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open directory " + f + " to read");
    }

    return new FSDataInputStream(
        new OzoneInputStream(getConf(), uri, bucket, pathToKey(f),
            fileStatus.getLen(), bufferSize, statistics));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress) throws IOException {
    LOG.trace("create() path:{}", f);
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
        deleteObject(key);
      }
    } catch (FileNotFoundException ignored) {
      // check if the parent directory needs to be created
      Path parent = f.getParent();
      try {
        // create all the directories for the parent
        FileStatus parentStatus = getFileStatus(parent);
        LOG.trace("parent key:{} status:{}", key, parentStatus);
      } catch (FileNotFoundException e) {
        mkdirs(parent);
      }
      // This exception needs to ignored as this means that the file currently
      // does not exists and a new file can thus be created.
    }

    final OzoneOutputStream stream =
        new OzoneOutputStream(getConf(), uri, bucket, key, this.statistics);
    // We pass null to FSDataOutputStream so it won't count writes that
    // are being buffered to a file
    return new FSDataOutputStream(stream, null);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    final Path parent = path.getParent();
    if (parent != null) {
      // expect this to raise an exception if there is no parent
      if (!getFileStatus(parent).isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }
    return create(path, permission, flags.contains(CreateFlag.OVERWRITE),
        bufferSize, replication, blockSize, progress);
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
      super(srcPath, true);
      srcKey = pathToKey(srcPath);
      dstKey = pathToKey(dstPath);
      LOG.trace("rename from:{} to:{}", srcKey, dstKey);
    }

    boolean processKey(String key) throws IOException {
      String newKeyName = dstKey.concat(key.substring(srcKey.length()));
      return rename(key, newKeyName);
    }

    // TODO: currently rename work by copying the file, with changes in KSM,
    // this operation can be made improved by renaming the keys in KSM directly.
    private boolean rename(String src, String dst) throws IOException {
      final LocalDirAllocator dirAlloc = new LocalDirAllocator(BUFFER_DIR_KEY);
      final File tmpFile = dirAlloc.createTmpFileForWrite("output-",
          LocalDirAllocator.SIZE_UNKNOWN, getConf());

      try {
        LOG.trace("rename by copying file from:{} to:{}", src, dst);
        bucket.getKey(src, tmpFile.toPath());
        bucket.putKey(dst, tmpFile);
        return true;
      } catch (OzoneException oe) {
        String msg = String.format("Error when renaming key from:%s to:%s",
            src, dst);
        LOG.error(msg, oe);
        throw new IOException(msg, oe);
      } finally {
        if (!tmpFile.delete()) {
          LOG.warn("Can not delete tmpFile: " + tmpFile);
        }
      }
    }
  }

  /**
   * Check whether the source and destination path are valid and then perform
   * rename by copying the data from source path to destination path.
   *
   * The rename operation is performed by copying data from source key
   * to destination key. This is done by reading the source key data into a
   * temporary file and then writing this temporary file to destination key.
   * The temporary file is deleted after the rename operation.
   * TODO: Optimize the operation by renaming keys in KSM.
   *
   * @param src source path for rename
   * @param dst destination path for rename
   * @return true if rename operation succeeded or
   * if the src and dst have the same path and are of the same type
   * @throws IOException on I/O errors or if the src/dst paths are invalid.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.trace("rename() from:{} to:{}", src, dst);

    if (src.isRoot()) {
      // Cannot rename root of file system
      LOG.trace("Cannot rename the root of a filesystem");
      return false;
    }

    // Cannot rename a directory to its own subdirectory
    Path parent = dst.getParent();
    while (parent != null && !src.equals(parent)) {
      parent = parent.getParent();
    }
    if (parent != null) {
      return false;
    }

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
      if (dst.toString().startsWith(src.toString())) {
        LOG.trace("Cannot rename a directory to a subdirectory of self");
        return false;
      }
    }
    RenameIterator iterator = new RenameIterator(src, dst);
    iterator.iterate();
    return src.equals(dst) || delete(src, true);
  }

  private class DeleteIterator extends OzoneListingIterator {
    private boolean recursive;
    DeleteIterator(Path f, boolean recursive)
        throws IOException {
      super(f, recursive);
      this.recursive = recursive;
    }

    boolean processKey(String key) throws IOException {
      if (key.equals("")) {
        LOG.trace("Skipping deleting root directory");
        return true;
      } else {
        LOG.trace("deleting key:" + key);
        boolean succeed = deleteObject(key);
        // if recursive delete is requested ignore the return value of
        // deleteObject and issue deletes for other keys.
        return recursive || succeed;
      }
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.trace("delete() path:{} recursive:{}", f, recursive);
    try {
      DeleteIterator iterator = new DeleteIterator(f, recursive);
      return iterator.iterate();
    } catch (FileNotFoundException e) {
      LOG.error("Couldn't delete {} - does not exist", f);
      return false;
    }
  }

  private class ListStatusIterator extends OzoneListingIterator {
    private  List<FileStatus> statuses = new ArrayList<>(LISTING_PAGE_SIZE);
    private Path f;

    ListStatusIterator(Path f) throws IOException  {
      super(f, true);
      this.f = f;
    }

    boolean processKey(String key) throws IOException {
      Path keyPath = new Path(OZONE_URI_DELIMITER + key);
      if (key.equals(getPathKey())) {
        if (pathIsDirectory()) {
          return true;
        } else {
          statuses.add(getFileStatus(keyPath));
          return true;
        }
      }
      // left with only subkeys now
      if (keyPath.getParent().getName().equals(f.getName())) {
        // skip keys which are for subdirectories of the directory
        statuses.add(getFileStatus(keyPath));
      }
      return true;
    }

    FileStatus[] getStatuses() {
      return statuses.toArray(new FileStatus[statuses.size()]);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
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
        if (!createDirectory(dirKey)) {
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
    if (StringUtils.isEmpty(key)) {
      return false;
    }
    return mkdir(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LOG.trace("getFileStatus() path:{}", f);
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);

    if (key.length() == 0) {
      return new FileStatus(0, true, 1, 0,
          getModifiedTime(bucket.getCreatedOn(), OZONE_URI_DELIMITER),
          qualifiedPath);
    }

    // consider this a file and get key status
    OzoneKey meta = getKeyInfo(key);
    if (meta == null) {
      key = addTrailingSlashIfNeeded(key);
      meta = getKeyInfo(key);
    }

    if (meta == null) {
      LOG.trace("File:{} not found", f);
      throw new FileNotFoundException(f + ": No such file or directory!");
    } else if (isDirectory(meta)) {
      return new FileStatus(0, true, 1, 0,
          getModifiedTime(meta.getObjectInfo().getModifiedOn(), key),
          qualifiedPath);
    } else {
      //TODO: Fetch replication count from ratis config
      return new FileStatus(meta.getObjectInfo().getSize(), false, 1,
            getDefaultBlockSize(f),
          getModifiedTime(meta.getObjectInfo().getModifiedOn(), key),
          qualifiedPath);
    }
  }

  /**
   * Helper method to fetch the key metadata info.
   * @param key key whose metadata information needs to be fetched
   * @return metadata info of the key
   */
  private OzoneKey getKeyInfo(String key) {
    try {
      return bucket.getKeyInfo(key);
    } catch (OzoneException e) {
      LOG.trace("Key:{} does not exists", key);
      return null;
    }
  }

  /**
   * Helper method to get the modified time of the key.
   * @param key key to fetch the modified time
   * @return last modified time of the key
   */
  private long getModifiedTime(String modifiedTime, String key) {
    try {
      return OzoneUtils.formatDate(modifiedTime);
    } catch (ParseException pe) {
      LOG.error("Invalid time:{} for key:{}", modifiedTime, key, pe);
      return 0;
    }
  }

  /**
   * Helper method to check if an Ozone key is representing a directory.
   * @param key key to be checked as a directory
   * @return true if key is a directory, false otherwise
   */
  private boolean isDirectory(OzoneKey key) {
    LOG.trace("key name:{} size:{}", key.getObjectInfo().getKeyName(),
        key.getObjectInfo().getSize());
    return key.getObjectInfo().getKeyName().endsWith(OZONE_URI_DELIMITER)
        && (key.getObjectInfo().getSize() == 0);
  }

  /**
   * Helper method to list entries matching the key name in bucket.
   * @param dirKey key prefix for listing the keys
   * @param lastKey last iterated key
   * @return List of Keys
   */
  List<OzoneKey> listKeys(String dirKey, String lastKey)
      throws IOException {
    LOG.trace("list keys dirKey:{} lastKey:{}", dirKey, lastKey);
    try {
      return bucket.listKeys(dirKey, LISTING_PAGE_SIZE, lastKey);
    } catch (OzoneException oe) {
      LOG.error("list keys failed dirKey:{} lastKey:{}", dirKey, lastKey, oe);
      throw new IOException("List keys failed " + oe.getMessage());
    }
  }

  /**
   * Helper method to create an directory specified by key name in bucket.
   * @param keyName key name to be created as directory
   * @return true if the key is created, false otherwise
   */
  private boolean createDirectory(String keyName) {
    try {
      LOG.trace("creating dir for key:{}", keyName);
      bucket.putKey(keyName, "");
      return true;
    } catch (OzoneException oe) {
      LOG.error("create key failed for key:{}", keyName, oe);
      return false;
    }
  }

  /**
   * Helper method to delete an object specified by key name in bucket.
   * @param keyName key name to be deleted
   * @return true if the key is deleted, false otherwise
   */
  private boolean deleteObject(String keyName) {
    LOG.trace("issuing delete for key" + keyName);
    try {
      bucket.deleteKey(keyName);
      return true;
    } catch (OzoneException oe) {
      LOG.error("delete key failed " + oe.getMessage());
      return false;
    }
  }

  /**
   * Turn a path (relative or otherwise) into an Ozone key.
   *
   * @param path the path of the file.
   * @return the key of the object that represents the file.
   */
  public String pathToKey(Path path) {
    Objects.requireNonNull(path, "Path can not be null!");
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
  String addTrailingSlashIfNeeded(String key) {
    if (StringUtils.isNotEmpty(key) && !key.endsWith(OZONE_URI_DELIMITER)) {
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

  private abstract class OzoneListingIterator {
    private final Path path;
    private final boolean recursive;
    private final FileStatus status;
    private String pathKey;

    OzoneListingIterator(Path path, boolean recursive)
        throws IOException {
      this.path = path;
      this.recursive = recursive;
      this.status = getFileStatus(path);
      this.pathKey = pathToKey(path);
      if (status.isDirectory()) {
        this.pathKey = addTrailingSlashIfNeeded(pathKey);
      }
    }

    abstract boolean processKey(String key) throws IOException;

    // iterates all the keys in the particular path
    boolean iterate() throws IOException {
      LOG.trace("Iterating path {} - recursive {}", path, recursive);
      if (status.isDirectory()) {
        LOG.trace("Iterating directory:{}", pathKey);
        String lastKey = pathKey;
        while (true) {
          List<OzoneKey> ozoneKeys = listKeys(pathKey, lastKey);
          LOG.trace("number of sub keys:{}", ozoneKeys.size());
          if (ozoneKeys.size() == 0) {
            return processKey(pathKey);
          } else {
            if (!recursive) {
              throw new PathIsNotEmptyDirectoryException(path.toString());
            } else {
              for (OzoneKey ozoneKey : ozoneKeys) {
                lastKey = ozoneKey.getObjectInfo().getKeyName();
                if (!processKey(lastKey)) {
                  return false;
                }
              }
            }
          }
        }
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
  }
}
