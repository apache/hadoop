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
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;

/**
 * Abstraction of filesystem operations that is essentially an interface
 * extracted from {@link FileContext}.
 */
public interface FSWrapper {

  abstract public void setWorkingDirectory(final Path newWDir)
      throws IOException;

  abstract public Path getWorkingDirectory();

  abstract public Path makeQualified(final Path path);

  abstract public FSDataOutputStream create(final Path f,
      final EnumSet<CreateFlag> createFlag, Options.CreateOpts... opts)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException;

  abstract public void mkdir(final Path dir, final FsPermission permission,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, IOException;

  abstract public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public FSDataInputStream open(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException,
      IOException;

  abstract public void rename(final Path src, final Path dst,
      final Options.Rename... options) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, IOException;

  abstract public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      UnsupportedFileSystemException, FileNotFoundException,
      IOException;

  abstract public void setTimes(final Path f, final long mtime, final long atime)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public FileChecksum getFileChecksum(final Path f)
      throws AccessControlException, FileNotFoundException, IOException;

  abstract public FileStatus getFileStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public FileStatus getFileLinkStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public Path getLinkTarget(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public BlockLocation[] getFileBlockLocations(final Path f,
      final long start, final long len) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException;

  abstract public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, IOException;

  abstract public RemoteIterator<FileStatus> listStatusIterator(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;

  abstract public FileStatus[] listStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException;
  
  abstract public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException;
}
