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
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.EnumSet;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.CreateOpts.BlockSize;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.junit.Assert;

/**
 * Helper class for unit tests.
 */
public final class FileContextTestWrapper extends FSTestWrapper {

  private final FileContext fc;

  public FileContextTestWrapper(FileContext context) {
    this(context, null);
  }

  public FileContextTestWrapper(FileContext context, String rootDir) {
    super(rootDir);
    this.fc = context;
  }

  public FSTestWrapper getLocalFSWrapper()
      throws UnsupportedFileSystemException {
    return new FileContextTestWrapper(FileContext.getLocalFSFileContext());
  }

  public Path getDefaultWorkingDirectory() throws IOException {
    return getTestRootPath("/user/" + System.getProperty("user.name"))
        .makeQualified(fc.getDefaultFileSystem().getUri(),
            fc.getWorkingDirectory());
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
      fc.create(path, EnumSet.of(CreateFlag.CREATE), options);
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
    out = fc.create(path, EnumSet.of(CreateFlag.APPEND));
    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
  }

  public boolean exists(Path p) throws IOException {
    return fc.util().exists(p);
  }

  public boolean isFile(Path p) throws IOException {
    try {
      return fc.getFileStatus(p).isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public boolean isDir(Path p) throws IOException {
    try {
      return fc.getFileStatus(p).isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public boolean isSymlink(Path p) throws IOException {
    try {
      return fc.getFileLinkStatus(p).isSymlink();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public void writeFile(Path path, byte b[]) throws IOException {
    FSDataOutputStream out =
      fc.create(path,EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent());
    out.write(b);
    out.close();
  }

  public byte[] readFile(Path path, int len) throws IOException {
    DataInputStream dis = fc.open(path);
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
    FileStatus s = fc.getFileStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDirectory());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(s.isFile());
    } else if (expectedType == fileType.isSymlink) {
      Assert.assertTrue(s.isSymlink());
    }
    Assert.assertEquals(fc.makeQualified(new Path(path)), s.getPath());
  }

  public void checkFileLinkStatus(String path, fileType expectedType)
      throws IOException {
    FileStatus s = fc.getFileLinkStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDirectory());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(s.isFile());
    } else if (expectedType == fileType.isSymlink) {
      Assert.assertTrue(s.isSymlink());
    }
    Assert.assertEquals(fc.makeQualified(new Path(path)), s.getPath());
  }

  //
  // FileContext wrappers
  //

  @Override
  public Path makeQualified(Path path) {
    return fc.makeQualified(path);
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    fc.mkdir(dir, permission, createParent);
  }

  @Override
  public boolean delete(Path f, boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fc.delete(f, recursive);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fc.getFileLinkStatus(f);
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    fc.createSymlink(target, link, createParent);
  }

  @Override
  public void setWorkingDirectory(Path newWDir) throws IOException {
    fc.setWorkingDirectory(newWDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return fc.getWorkingDirectory();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fc.getFileStatus(f);
  }

  @Override
  public FSDataOutputStream create(Path f, EnumSet<CreateFlag> createFlag,
      CreateOpts... opts) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
    return fc.create(f, createFlag, opts);
  }

  @Override
  public FSDataInputStream open(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fc.open(f);
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException,
      IOException {
    return fc.setReplication(f, replication);
  }

  @Override
  public Path getLinkTarget(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fc.getLinkTarget(f);
  }

  @Override
  public void rename(Path src, Path dst, Rename... options)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    fc.rename(src, dst, options);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fc.getFileBlockLocations(f, start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    return fc.getFileChecksum(f);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fc.listStatus(f);
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    fc.setPermission(f, permission);
  }

  @Override
  public void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      UnsupportedFileSystemException, FileNotFoundException,
      IOException {
    fc.setOwner(f, username, groupname);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    fc.setTimes(f, mtime, atime);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    return fc.util().listStatus(f);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    return fc.util().globStatus(pathPattern, filter);
  }
}
