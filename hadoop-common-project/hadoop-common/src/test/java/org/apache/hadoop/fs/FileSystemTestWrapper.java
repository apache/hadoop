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

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.CreateOpts.BlockSize;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;

/**
 * Helper class for unit tests.
 */
public final class FileSystemTestWrapper extends FSTestWrapper {

  private final FileSystem fs;

  public FileSystemTestWrapper(FileSystem fs) {
    this(fs, null);
  }

  public FileSystemTestWrapper(FileSystem fs, String rootDir) {
    super(rootDir);
    this.fs = fs;
  }

  public FSTestWrapper getLocalFSWrapper()
      throws IOException {
    return new FileSystemTestWrapper(FileSystem.getLocal(fs.getConf()));
  }

  public Path getDefaultWorkingDirectory() throws IOException {
    return getTestRootPath("/user/" + System.getProperty("user.name"))
        .makeQualified(fs.getUri(),
            fs.getWorkingDirectory());
  }

  /*
   * Create files with numBlocks blocks each with block size blockSize.
   */
  public long createFile(Path path, int numBlocks, CreateOpts... options)
      throws IOException {
    BlockSize blockSizeOpt = CreateOpts.getOpt(CreateOpts.BlockSize.class, options);
    long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue()
        : DEFAULT_BLOCK_SIZE;
    FSDataOutputStream out =
      create(path, EnumSet.of(CreateFlag.CREATE), options);
    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
    return data.length;
  }

  public long createFile(Path path, int numBlocks, int blockSize)
      throws IOException {
    return createFile(path, numBlocks, CreateOpts.blockSize(blockSize),
        CreateOpts.createParent());
  }

  public long createFile(Path path) throws IOException {
    return createFile(path, DEFAULT_NUM_BLOCKS, CreateOpts.createParent());
  }

  public long createFile(String name) throws IOException {
    Path path = getTestRootPath(name);
    return createFile(path);
  }

  public long createFileNonRecursive(String name) throws IOException {
    Path path = getTestRootPath(name);
    return createFileNonRecursive(path);
  }

  public long createFileNonRecursive(Path path) throws IOException {
    return createFile(path, DEFAULT_NUM_BLOCKS, CreateOpts.donotCreateParent());
  }

  public void appendToFile(Path path, int numBlocks, CreateOpts... options)
      throws IOException {
    BlockSize blockSizeOpt = CreateOpts.getOpt(CreateOpts.BlockSize.class, options);
    long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue()
        : DEFAULT_BLOCK_SIZE;
    FSDataOutputStream out;
    out = fs.append(path);
    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
  }

  public boolean exists(Path p) throws IOException {
    return fs.exists(p);
  }

  public boolean isFile(Path p) throws IOException {
    try {
      return fs.getFileStatus(p).isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public boolean isDir(Path p) throws IOException {
    try {
      return fs.getFileStatus(p).isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public boolean isSymlink(Path p) throws IOException {
    try {
      return fs.getFileLinkStatus(p).isSymlink();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public void writeFile(Path path, byte b[]) throws IOException {
    FSDataOutputStream out =
      create(path,EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent());
    out.write(b);
    out.close();
  }

  public byte[] readFile(Path path, int len) throws IOException {
    DataInputStream dis = fs.open(path);
    byte[] buffer = new byte[len];
    IOUtils.readFully(dis, buffer, 0, len);
    dis.close();
    return buffer;
  }

  public FileStatus containsPath(Path path, FileStatus[] dirList)
    throws IOException {
    for(int i = 0; i < dirList.length; i ++) {
      if (path.equals(dirList[i].getPath()))
        return dirList[i];
      }
    return null;
  }

  public FileStatus containsPath(String path, FileStatus[] dirList)
     throws IOException {
    return containsPath(new Path(path), dirList);
  }

  public void checkFileStatus(String path, fileType expectedType)
      throws IOException {
    FileStatus s = fs.getFileStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDirectory());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(s.isFile());
    } else if (expectedType == fileType.isSymlink) {
      Assert.assertTrue(s.isSymlink());
    }
    Assert.assertEquals(fs.makeQualified(new Path(path)), s.getPath());
  }

  public void checkFileLinkStatus(String path, fileType expectedType)
      throws IOException {
    FileStatus s = fs.getFileLinkStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDirectory());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(s.isFile());
    } else if (expectedType == fileType.isSymlink) {
      Assert.assertTrue(s.isSymlink());
    }
    Assert.assertEquals(fs.makeQualified(new Path(path)), s.getPath());
  }

  //
  // FileContext wrappers
  //

  @Override
  public Path makeQualified(Path path) {
    return fs.makeQualified(path);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    fs.primitiveMkdir(dir, permission, createParent);
  }

  @Override
  public boolean delete(Path f, boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fs.delete(f, recursive);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fs.getFileLinkStatus(f);
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    fs.createSymlink(target, link, createParent);
  }

  @Override
  public void setWorkingDirectory(Path newWDir) throws IOException {
    fs.setWorkingDirectory(newWDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return fs.getWorkingDirectory();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fs.getFileStatus(f);
  }

  @Override
  public FSDataOutputStream create(Path f, EnumSet<CreateFlag> createFlag,
      CreateOpts... opts) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, IOException {

    // Need to translate the FileContext-style options into FileSystem-style

    // Permissions with umask
    CreateOpts.Perms permOpt = CreateOpts.getOpt(
        CreateOpts.Perms.class, opts);
    FsPermission umask = FsPermission.getUMask(fs.getConf());
    FsPermission permission = (permOpt != null) ? permOpt.getValue()
        : FsPermission.getFileDefault().applyUMask(umask);
    permission = permission.applyUMask(umask);
    // Overwrite
    boolean overwrite = createFlag.contains(CreateFlag.OVERWRITE);
    // bufferSize
    int bufferSize = fs.getConf().getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    CreateOpts.BufferSize bufOpt = CreateOpts.getOpt(
        CreateOpts.BufferSize.class, opts);
    bufferSize = (bufOpt != null) ? bufOpt.getValue() : bufferSize;
    // replication
    short replication = fs.getDefaultReplication(f);
    CreateOpts.ReplicationFactor repOpt =
        CreateOpts.getOpt(CreateOpts.ReplicationFactor.class, opts);
    replication = (repOpt != null) ? repOpt.getValue() : replication;
    // blockSize
    long blockSize = fs.getDefaultBlockSize(f);
    CreateOpts.BlockSize blockOpt = CreateOpts.getOpt(
        CreateOpts.BlockSize.class, opts);
    blockSize = (blockOpt != null) ? blockOpt.getValue() : blockSize;
    // Progressable
    Progressable progress = null;
    CreateOpts.Progress progressOpt = CreateOpts.getOpt(
        CreateOpts.Progress.class, opts);
    progress = (progressOpt != null) ? progressOpt.getValue() : progress;
    return fs.create(f, permission, overwrite, bufferSize, replication,
        blockSize, progress);
  }

  @Override
  public FSDataInputStream open(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fs.open(f);
  }

  @Override
  public Path getLinkTarget(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fs.getLinkTarget(f);
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException,
      IOException {
    return fs.setReplication(f, replication);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, Rename... options)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    fs.rename(src, dst, options);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fs.getFileBlockLocations(f, start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    return fs.getFileChecksum(f);
  }

  private class FakeRemoteIterator<E> implements RemoteIterator<E> {

    private E[] elements;
    private int count;

    FakeRemoteIterator(E[] elements) {
      this.elements = elements;
      count = 0;
    }

    @Override
    public boolean hasNext() throws IOException {
      return count < elements.length;
    }

    @Override
    public E next() throws IOException {
      if (hasNext()) {
        return elements[count++];
      }
      return null;
    }
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    // Fake the RemoteIterator, because FileSystem has no such thing
    FileStatus[] statuses = fs.listStatus(f);
    return new FakeRemoteIterator<FileStatus>(statuses);
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    fs.setPermission(f, permission);
  }

  @Override
  public void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      UnsupportedFileSystemException, FileNotFoundException,
      IOException {
    fs.setOwner(f, username, groupname);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    fs.setTimes(f, mtime, atime);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fs.listStatus(f);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    return fs.globStatus(pathPattern, filter);
  }
}
