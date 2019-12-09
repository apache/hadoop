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

package org.apache.hadoop.registry.client.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Filesystem-based implementation of RegistryOperations. This class relies
 * entirely on the configured FS for security and does no extra checks.
 */
public class FSRegistryOperationsService extends CompositeService
    implements RegistryOperations {

  private FileSystem fs;
  private static final Logger LOG =
      LoggerFactory.getLogger(FSRegistryOperationsService.class);
  private final RegistryUtils.ServiceRecordMarshal serviceRecordMarshal =
      new RegistryUtils.ServiceRecordMarshal();

  public FSRegistryOperationsService() {
    super(FSRegistryOperationsService.class.getName());
  }

  @VisibleForTesting
  public FileSystem getFs() {
    return this.fs;
  }

  @Override
  protected void serviceInit(Configuration conf) {
    try {
      this.fs = FileSystem.get(conf);
      LOG.info("Initialized Yarn-registry with Filesystem "
          + fs.getClass().getCanonicalName());
    } catch (IOException e) {
      LOG.error("Failed to get FileSystem for registry", e);
      throw new YarnRuntimeException(e);
    }
  }

  private Path makePath(String path) {
    return new Path(path);
  }

  private Path formatDataPath(String basePath) {
    return Path.mergePaths(new Path(basePath), new Path("/_record"));
  }

  private String relativize(String basePath, String childPath) {
    String relative = new File(basePath).toURI()
        .relativize(new File(childPath).toURI()).getPath();
    return relative;
  }

  @Override
  public boolean mknode(String path, boolean createParents)
      throws PathNotFoundException, InvalidPathnameException, IOException {
    Path registryPath = makePath(path);

    // getFileStatus throws FileNotFound if the path doesn't exist. If the
    // file already exists, return.
    try {
      fs.getFileStatus(registryPath);
      return false;
    } catch (FileNotFoundException e) {
    }

    if (createParents) {
      // By default, mkdirs creates any parent dirs it needs
      fs.mkdirs(registryPath);
    } else {
      FileStatus parentStatus = null;

      if (registryPath.getParent() != null) {
        parentStatus = fs.getFileStatus(registryPath.getParent());
      }

      if (registryPath.getParent() == null || parentStatus.isDirectory()) {
        fs.mkdirs(registryPath);
      } else {
        throw new PathNotFoundException("no parent for " + path);
      }
    }
    return true;
  }

  @Override
  public void bind(String path, ServiceRecord record, int flags)
      throws PathNotFoundException, FileAlreadyExistsException,
      InvalidPathnameException, IOException {

    // Preserve same overwrite semantics as ZK implementation
    Preconditions.checkArgument(record != null, "null record");
    RegistryTypeUtils.validateServiceRecord(path, record);

    Path dataPath = formatDataPath(path);
    Boolean overwrite = ((flags & BindFlags.OVERWRITE) != 0);
    if (fs.exists(dataPath) && !overwrite) {
      throw new FileAlreadyExistsException();
    } else {
      // Either the file doesn't exist, or it exists and we're
      // overwriting. Create overwrites by default and creates parent dirs if
      // needed.
      FSDataOutputStream stream = fs.create(dataPath);
      byte[] bytes = serviceRecordMarshal.toBytes(record);
      stream.write(bytes);
      stream.close();
      LOG.info("Bound record to path " + dataPath);
    }
  }

  @Override
  public ServiceRecord resolve(String path) throws PathNotFoundException,
      NoRecordException, InvalidRecordException, IOException {
    // Read the entire file into byte array, should be small metadata

    Long size = fs.getFileStatus(formatDataPath(path)).getLen();
    byte[] bytes = new byte[size.intValue()];

    FSDataInputStream instream = fs.open(formatDataPath(path));
    int bytesRead = instream.read(bytes);
    instream.close();

    if (bytesRead < size) {
      throw new InvalidRecordException(path,
          "Expected " + size + " bytes, but read " + bytesRead);
    }

    // Unmarshal, check, and return
    ServiceRecord record = serviceRecordMarshal.fromBytes(path, bytes);
    RegistryTypeUtils.validateServiceRecord(path, record);
    return record;
  }

  @Override
  public RegistryPathStatus stat(String path)
      throws PathNotFoundException, InvalidPathnameException, IOException {
    FileStatus fstat = fs.getFileStatus(formatDataPath(path));
    int numChildren = fs.listStatus(makePath(path)).length;

    RegistryPathStatus regstat =
        new RegistryPathStatus(fstat.getPath().toString(),
            fstat.getModificationTime(), fstat.getLen(), numChildren);

    return regstat;
  }

  @Override
  public boolean exists(String path) throws IOException {
    return fs.exists(makePath(path));
  }

  @Override
  public List<String> list(String path)
      throws PathNotFoundException, InvalidPathnameException, IOException {
    FileStatus[] statArray = fs.listStatus(makePath(path));
    String basePath = fs.getFileStatus(makePath(path)).getPath().toString();

    List<String> paths = new ArrayList<String>();

    FileStatus stat;
    // Only count dirs; the _record files are hidden.
    for (int i = 0; i < statArray.length; i++) {
      stat = statArray[i];
      if (stat.isDirectory()) {
        String relativePath = relativize(basePath, stat.getPath().toString());
        paths.add(relativePath);
      }
    }

    return paths;
  }

  @Override
  public void delete(String path, boolean recursive)
      throws PathNotFoundException, PathIsNotEmptyDirectoryException,
      InvalidPathnameException, IOException {
    Path dirPath = makePath(path);
    if (!fs.exists(dirPath)) {
      throw new PathNotFoundException(path);
    }

    // If recursive == true, or dir is empty, delete.
    if (recursive || list(path).isEmpty()) {
      fs.delete(makePath(path), true);
      return;
    }

    throw new PathIsNotEmptyDirectoryException(path);
  }

  @Override
  public boolean addWriteAccessor(String id, String pass) throws IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public void clearWriteAccessors() {
    throw new NotImplementedException("Code is not implemented");
  }

}
