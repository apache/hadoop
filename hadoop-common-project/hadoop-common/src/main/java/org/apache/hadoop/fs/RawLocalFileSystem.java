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

import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * Implement the FileSystem API for the raw local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RawLocalFileSystem extends FileSystem {
  static final URI NAME = URI.create("file:///");
  private Path workingDir;
  // Temporary workaround for HADOOP-9652.
  private static boolean useDeprecatedFileStatus = true;

  @VisibleForTesting
  public static void useStatIfAvailable() {
    useDeprecatedFileStatus = !Stat.isAvailable();
  }
  
  public RawLocalFileSystem() {
    workingDir = getInitialWorkingDirectory();
  }
  
  private Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new Path(workingDir, f);
    }
  }
  
  /** Convert a path to a File. */
  public File pathToFile(Path path) {
    checkPath(path);
    if (!path.isAbsolute()) {
      path = new Path(getWorkingDirectory(), path);
    }
    return new File(path.toUri().getPath());
  }

  @Override
  public URI getUri() { return NAME; }
  
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
  }
  
  /*******************************************************
   * For open()'s FSInputStream.
   *******************************************************/
  class LocalFSFileInputStream extends FSInputStream implements HasFileDescriptor {
    private FileInputStream fis;
    private long position;

    public LocalFSFileInputStream(Path f) throws IOException {
      fis = new FileInputStream(pathToFile(f));
    }
    
    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0) {
        throw new EOFException(
          FSExceptionMessages.NEGATIVE_SEEK);
      }
      fis.getChannel().position(pos);
      this.position = pos;
    }
    
    @Override
    public long getPos() throws IOException {
      return this.position;
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
    
    /*
     * Just forward to the fis
     */
    @Override
    public int available() throws IOException { return fis.available(); }
    @Override
    public void close() throws IOException { fis.close(); }
    @Override
    public boolean markSupported() { return false; }
    
    @Override
    public int read() throws IOException {
      try {
        int value = fis.read();
        if (value >= 0) {
          this.position++;
          statistics.incrementBytesRead(1);
        }
        return value;
      } catch (IOException e) {                 // unexpected exception
        throw new FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      // parameter check
      validatePositionedReadArgs(position, b, off, len);
      try {
        int value = fis.read(b, off, len);
        if (value > 0) {
          this.position += value;
          statistics.incrementBytesRead(value);
        }
        return value;
      } catch (IOException e) {                 // unexpected exception
        throw new FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public int read(long position, byte[] b, int off, int len)
      throws IOException {
      // parameter check
      validatePositionedReadArgs(position, b, off, len);
      if (len == 0) {
        return 0;
      }

      ByteBuffer bb = ByteBuffer.wrap(b, off, len);
      try {
        int value = fis.getChannel().read(bb, position);
        if (value > 0) {
          statistics.incrementBytesRead(value);
        }
        return value;
      } catch (IOException e) {
        throw new FSError(e);
      }
    }
    
    @Override
    public long skip(long n) throws IOException {
      long value = fis.skip(n);
      if (value > 0) {
        this.position += value;
      }
      return value;
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
      return fis.getFD();
    }
  }
  
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    getFileStatus(f);
    return new FSDataInputStream(new BufferedFSInputStream(
        new LocalFSFileInputStream(f), bufferSize));
  }
  
  /*********************************************************
   * For create()'s FSOutputStream.
   *********************************************************/
  class LocalFSFileOutputStream extends OutputStream {
    private FileOutputStream fos;
    
    private LocalFSFileOutputStream(Path f, boolean append,
        FsPermission permission) throws IOException {
      File file = pathToFile(f);
      if (!append && permission == null) {
        permission = FsPermission.getFileDefault();
      }
      if (permission == null) {
        this.fos = new FileOutputStream(file, append);
      } else {
        permission = permission.applyUMask(FsPermission.getUMask(getConf()));
        if (Shell.WINDOWS && NativeIO.isAvailable()) {
          this.fos = NativeIO.Windows.createFileOutputStreamWithMode(file,
              append, permission.toShort());
        } else {
          this.fos = new FileOutputStream(file, append);
          boolean success = false;
          try {
            setPermission(f, permission);
            success = true;
          } finally {
            if (!success) {
              IOUtils.cleanup(LOG, this.fos);
            }
          }
        }
      }
    }
    
    /*
     * Just forward to the fos
     */
    @Override
    public void close() throws IOException { fos.close(); }
    @Override
    public void flush() throws IOException { fos.flush(); }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        fos.write(b, off, len);
      } catch (IOException e) {                // unexpected exception
        throw new FSError(e);                  // assume native fs error
      }
    }
    
    @Override
    public void write(int b) throws IOException {
      try {
        fos.write(b);
      } catch (IOException e) {              // unexpected exception
        throw new FSError(e);                // assume native fs error
      }
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    FileStatus status = getFileStatus(f);
    if (status.isDirectory()) {
      throw new IOException("Cannot append to a diretory (=" + f + " )");
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        createOutputStreamWithMode(f, true, null), bufferSize), statistics,
        status.getLen());
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
    short replication, long blockSize, Progressable progress)
    throws IOException {
    return create(f, overwrite, true, bufferSize, replication, blockSize,
        progress, null);
  }

  private FSDataOutputStream create(Path f, boolean overwrite,
      boolean createParent, int bufferSize, short replication, long blockSize,
      Progressable progress, FsPermission permission) throws IOException {
    if (exists(f) && !overwrite) {
      throw new FileAlreadyExistsException("File already exists: " + f);
    }
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent.toString());
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        createOutputStreamWithMode(f, false, permission), bufferSize),
        statistics);
  }
  
  protected OutputStream createOutputStream(Path f, boolean append) 
      throws IOException {
    return createOutputStreamWithMode(f, append, null);
  }

  protected OutputStream createOutputStreamWithMode(Path f, boolean append,
      FsPermission permission) throws IOException {
    return new LocalFSFileOutputStream(f, append, permission);
  }
  
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
      throw new FileAlreadyExistsException("File already exists: " + f);
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        createOutputStreamWithMode(f, false, permission), bufferSize),
            statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite, int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {

    FSDataOutputStream out = create(f, overwrite, true, bufferSize, replication,
        blockSize, progress, permission);
    return out;
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    FSDataOutputStream out = create(f, overwrite, false, bufferSize, replication,
        blockSize, progress, permission);
    return out;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // Attempt rename using Java API.
    File srcFile = pathToFile(src);
    File dstFile = pathToFile(dst);
    if (srcFile.renameTo(dstFile)) {
      return true;
    }

    // Else try POSIX style rename on Windows only
    if (Shell.WINDOWS &&
        handleEmptyDstDirectoryOnWindows(src, srcFile, dst, dstFile)) {
      return true;
    }

    // The fallback behavior accomplishes the rename by a full copy.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Falling through to a copy of " + src + " to " + dst);
    }
    return FileUtil.copy(this, src, this, dst, true, getConf());
  }

  @VisibleForTesting
  public final boolean handleEmptyDstDirectoryOnWindows(Path src, File srcFile,
      Path dst, File dstFile) throws IOException {

    // Enforce POSIX rename behavior that a source directory replaces an
    // existing destination if the destination is an empty directory. On most
    // platforms, this is already handled by the Java API call above. Some
    // platforms (notably Windows) do not provide this behavior, so the Java API
    // call renameTo(dstFile) fails. Delete destination and attempt rename
    // again.
    try {
      FileStatus sdst = this.getFileStatus(dst);
      if (sdst.isDirectory() && dstFile.list().length == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deleting empty destination and renaming " + src + " to " +
              dst);
        }
        if (this.delete(dst, false) && srcFile.renameTo(dstFile)) {
          return true;
        }
      }
    } catch (FileNotFoundException ignored) {
    }
    return false;
  }

  @Override
  public boolean truncate(Path f, final long newLength) throws IOException {
    FileStatus status = getFileStatus(f);
    if(status == null) {
      throw new FileNotFoundException("File " + f + " not found");
    }
    if(status.isDirectory()) {
      throw new IOException("Cannot truncate a directory (=" + f + ")");
    }
    long oldLength = status.getLen();
    if(newLength > oldLength) {
      throw new IllegalArgumentException(
          "Cannot truncate to a larger file size. Current size: " + oldLength +
          ", truncate size: " + newLength + ".");
    }
    try (FileOutputStream out = new FileOutputStream(pathToFile(f), true)) {
      try {
        out.getChannel().truncate(newLength);
      } catch(IOException e) {
        throw new FSError(e);
      }
    }
    return true;
  }
  
  /**
   * Delete the given path to a file or directory.
   * @param p the path to delete
   * @param recursive to delete sub-directories
   * @return true if the file or directory and all its contents were deleted
   * @throws IOException if p is non-empty and recursive is false 
   */
  @Override
  public boolean delete(Path p, boolean recursive) throws IOException {
    File f = pathToFile(p);
    if (!f.exists()) {
      //no path, return false "nothing to delete"
      return false;
    }
    if (f.isFile()) {
      return f.delete();
    } else if (!recursive && f.isDirectory() && 
        (FileUtil.listFiles(f).length != 0)) {
      throw new IOException("Directory " + f.toString() + " is not empty");
    }
    return FileUtil.fullyDelete(f);
  }
 
  /**
   * {@inheritDoc}
   *
   * (<b>Note</b>: Returned list is not sorted in any given order,
   * due to reliance on Java's {@link File#list()} API.)
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    File localf = pathToFile(f);
    FileStatus[] results;

    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }

    if (localf.isDirectory()) {
      String[] names = FileUtil.list(localf);
      results = new FileStatus[names.length];
      int j = 0;
      for (int i = 0; i < names.length; i++) {
        try {
          // Assemble the path using the Path 3 arg constructor to make sure
          // paths with colon are properly resolved on Linux
          results[j] = getFileStatus(new Path(f, new Path(null, null,
                                                          names[i])));
          j++;
        } catch (FileNotFoundException e) {
          // ignore the files not found since the dir list may have have
          // changed since the names[] list was generated.
        }
      }
      if (j == names.length) {
        return results;
      }
      return Arrays.copyOf(results, j);
    }

    if (!useDeprecatedFileStatus) {
      return new FileStatus[] { getFileStatus(f) };
    }
    return new FileStatus[] {
        new DeprecatedRawLocalFileStatus(localf,
        getDefaultBlockSize(f), this) };
  }
  
  protected boolean mkOneDir(File p2f) throws IOException {
    return mkOneDirWithMode(new Path(p2f.getAbsolutePath()), p2f, null);
  }

  protected boolean mkOneDirWithMode(Path p, File p2f, FsPermission permission)
      throws IOException {
    if (permission == null) {
      permission = FsPermission.getDirDefault();
    }
    permission = permission.applyUMask(FsPermission.getUMask(getConf()));
    if (Shell.WINDOWS && NativeIO.isAvailable()) {
      try {
        NativeIO.Windows.createDirectoryWithMode(p2f, permission.toShort());
        return true;
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format(
              "NativeIO.createDirectoryWithMode error, path = %s, mode = %o",
              p2f, permission.toShort()), e);
        }
        return false;
      }
    } else {
      boolean b = p2f.mkdir();
      if (b) {
        setPermission(p, permission);
      }
      return b;
    }
  }

  /**
   * Creates the specified directory hierarchy. Does not
   * treat existence as an error.
   */
  @Override
  public boolean mkdirs(Path f) throws IOException {
    return mkdirsWithOptionalPermission(f, null);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mkdirsWithOptionalPermission(f, permission);
  }

  private boolean mkdirsWithOptionalPermission(Path f, FsPermission permission)
      throws IOException {
    if(f == null) {
      throw new IllegalArgumentException("mkdirs path arg is null");
    }
    Path parent = f.getParent();
    File p2f = pathToFile(f);
    File parent2f = null;
    if(parent != null) {
      parent2f = pathToFile(parent);
      if(parent2f != null && parent2f.exists() && !parent2f.isDirectory()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + parent);
      }
    }
    if (p2f.exists() && !p2f.isDirectory()) {
      throw new FileNotFoundException("Destination exists" +
              " and is not a directory: " + p2f.getCanonicalPath());
    }
    return (parent == null || parent2f.exists() || mkdirs(parent)) &&
      (mkOneDirWithMode(f, p2f, permission) || p2f.isDirectory());
  }
  
  
  @Override
  public Path getHomeDirectory() {
    return this.makeQualified(new Path(System.getProperty("user.home")));
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
    checkPath(workingDir);
  }
  
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }
  
  @Override
  protected Path getInitialWorkingDirectory() {
    return this.makeQualified(new Path(System.getProperty("user.dir")));
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    File partition = pathToFile(p == null ? new Path("/") : p);
    //File provides getUsableSpace() and getFreeSpace()
    //File provides no API to obtain used space, assume used = total - free
    return new FsStatus(partition.getTotalSpace(), 
      partition.getTotalSpace() - partition.getFreeSpace(),
      partition.getFreeSpace());
  }
  
  // In the case of the local filesystem, we can just rename the file.
  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    rename(src, dst);
  }
  
  // We can write output directly to the final location
  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return fsOutputFile;
  }
  
  // It's in the right place - nothing to do.
  @Override
  public void completeLocalOutput(Path fsWorkingFile, Path tmpLocalFile)
    throws IOException {
  }
  
  @Override
  public void close() throws IOException {
    super.close();
  }
  
  @Override
  public String toString() {
    return "LocalFS";
  }
  
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return getFileLinkStatusInternal(f, true);
  }

  @Deprecated
  private FileStatus deprecatedGetFileStatus(Path f) throws IOException {
    File path = pathToFile(f);
    if (path.exists()) {
      return new DeprecatedRawLocalFileStatus(pathToFile(f),
          getDefaultBlockSize(f), this);
    } else {
      throw new FileNotFoundException("File " + f + " does not exist");
    }
  }

  @Deprecated
  static class DeprecatedRawLocalFileStatus extends FileStatus {
    /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
     * We recognize if the information is already loaded by check if
     * onwer.equals("").
     */
    private boolean isPermissionLoaded() {
      return !super.getOwner().isEmpty(); 
    }

    private static long getLastAccessTime(File f) throws IOException {
      long accessTime;
      try {
        accessTime = Files.readAttributes(f.toPath(),
            BasicFileAttributes.class).lastAccessTime().toMillis();
      } catch (NoSuchFileException e) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      return accessTime;
    }

    DeprecatedRawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs)
      throws IOException {
      super(f.length(), f.isDirectory(), 1, defaultBlockSize,
          f.lastModified(), getLastAccessTime(f),
          null, null, null,
          new Path(f.getPath()).makeQualified(fs.getUri(),
            fs.getWorkingDirectory()));
    }
    
    @Override
    public FsPermission getPermission() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getPermission();
    }

    @Override
    public String getOwner() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getOwner();
    }

    @Override
    public String getGroup() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getGroup();
    }

    /// loads permissions, owner, and group from `ls -ld`
    private void loadPermissionInfo() {
      IOException e = null;
      try {
        String output = FileUtil.execCommand(new File(getPath().toUri()), 
            Shell.getGetPermissionCommand());
        StringTokenizer t =
            new StringTokenizer(output, Shell.TOKEN_SEPARATOR_REGEX);
        //expected format
        //-rw-------    1 username groupname ...
        String permission = t.nextToken();
        if (permission.length() > FsPermission.MAX_PERMISSION_LENGTH) {
          //files with ACLs might have a '+'
          permission = permission.substring(0,
            FsPermission.MAX_PERMISSION_LENGTH);
        }
        setPermission(FsPermission.valueOf(permission));
        t.nextToken();

        String owner = t.nextToken();
        // If on windows domain, token format is DOMAIN\\user and we want to
        // extract only the user name
        if (Shell.WINDOWS) {
          int i = owner.indexOf('\\');
          if (i != -1)
            owner = owner.substring(i + 1);
        }
        setOwner(owner);

        setGroup(t.nextToken());
      } catch (Shell.ExitCodeException ioe) {
        if (ioe.getExitCode() != 1) {
          e = ioe;
        } else {
          setPermission(null);
          setOwner(null);
          setGroup(null);
        }
      } catch (IOException ioe) {
        e = ioe;
      } finally {
        if (e != null) {
          throw new RuntimeException("Error while running command to get " +
                                     "file permissions : " + 
                                     StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      super.write(out);
    }
  }

  /**
   * Use the command chown to set owner.
   */
  @Override
  public void setOwner(Path p, String username, String groupname)
    throws IOException {
    FileUtil.setOwner(pathToFile(p), username, groupname);
  }

  /**
   * Use the command chmod to set permission.
   */
  @Override
  public void setPermission(Path p, FsPermission permission)
    throws IOException {
    if (NativeIO.isAvailable()) {
      NativeIO.POSIX.chmod(pathToFile(p).getCanonicalPath(),
                     permission.toShort());
    } else {
      String perm = String.format("%04o", permission.toShort());
      Shell.execCommand(Shell.getSetPermissionCommand(perm, false,
        FileUtil.makeShellPath(pathToFile(p), true)));
    }
  }
 
  /**
   * Sets the {@link Path}'s last modified time and last access time to
   * the given valid times.
   *
   * @param mtime the modification time to set (only if no less than zero).
   * @param atime the access time to set (only if no less than zero).
   * @throws IOException if setting the times fails.
   */
  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    try {
      BasicFileAttributeView view = Files.getFileAttributeView(
          pathToFile(p).toPath(), BasicFileAttributeView.class);
      FileTime fmtime = (mtime >= 0) ? FileTime.fromMillis(mtime) : null;
      FileTime fatime = (atime >= 0) ? FileTime.fromMillis(atime) : null;
      view.setTimes(fmtime, fatime, null);
    } catch (NoSuchFileException e) {
      throw new FileNotFoundException("File " + p + " does not exist");
    }
  }

  @Override
  public boolean supportsSymlinks() {
    return true;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws IOException {
    if (!FileSystem.areSymlinksEnabled()) {
      throw new UnsupportedOperationException("Symlinks not supported");
    }
    final String targetScheme = target.toUri().getScheme();
    if (targetScheme != null && !"file".equals(targetScheme)) {
      throw new IOException("Unable to create symlink to non-local file "+
                            "system: "+target.toString());
    }
    if (createParent) {
      mkdirs(link.getParent());
    }

    // NB: Use createSymbolicLink in java.nio.file.Path once available
    int result = FileUtil.symLink(target.toString(),
        makeAbsolute(link).toString());
    if (result != 0) {
      throw new IOException("Error " + result + " creating symlink " +
          link + " to " + target);
    }
  }

  /**
   * Return a FileStatus representing the given path. If the path refers
   * to a symlink return a FileStatus representing the link rather than
   * the object the link refers to.
   */
  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // getFileLinkStatus is supposed to return a symlink with a
    // qualified path
    if (fi.isSymlink()) {
      Path targetQual = FSLinkResolver.qualifySymlinkTarget(this.getUri(),
          fi.getPath(), fi.getSymlink());
      fi.setSymlink(targetQual);
    }
    return fi;
  }

  /**
   * Public {@link FileStatus} methods delegate to this function, which in turn
   * either call the new {@link Stat} based implementation or the deprecated
   * methods based on platform support.
   * 
   * @param f Path to stat
   * @param dereference whether to dereference the final path component if a
   *          symlink
   * @return FileStatus of f
   * @throws IOException
   */
  private FileStatus getFileLinkStatusInternal(final Path f,
      boolean dereference) throws IOException {
    if (!useDeprecatedFileStatus) {
      return getNativeFileLinkStatus(f, dereference);
    } else if (dereference) {
      return deprecatedGetFileStatus(f);
    } else {
      return deprecatedGetFileLinkStatusInternal(f);
    }
  }

  /**
   * Deprecated. Remains for legacy support. Should be removed when {@link Stat}
   * gains support for Windows and other operating systems.
   */
  @Deprecated
  private FileStatus deprecatedGetFileLinkStatusInternal(final Path f)
      throws IOException {
    String target = FileUtil.readLink(new File(f.toString()));

    try {
      FileStatus fs = getFileStatus(f);
      // If f refers to a regular file or directory
      if (target.isEmpty()) {
        return fs;
      }
      // Otherwise f refers to a symlink
      return new FileStatus(fs.getLen(),
          false,
          fs.getReplication(),
          fs.getBlockSize(),
          fs.getModificationTime(),
          fs.getAccessTime(),
          fs.getPermission(),
          fs.getOwner(),
          fs.getGroup(),
          new Path(target),
          f);
    } catch (FileNotFoundException e) {
      /* The exists method in the File class returns false for dangling
       * links so we can get a FileNotFoundException for links that exist.
       * It's also possible that we raced with a delete of the link. Use
       * the readBasicFileAttributes method in java.nio.file.attributes
       * when available.
       */
      if (!target.isEmpty()) {
        return new FileStatus(0, false, 0, 0, 0, 0, FsPermission.getDefault(),
            "", "", new Path(target), f);
      }
      // f refers to a file or directory that does not exist
      throw e;
    }
  }
  /**
   * Calls out to platform's native stat(1) implementation to get file metadata
   * (permissions, user, group, atime, mtime, etc). This works around the lack
   * of lstat(2) in Java 6.
   * 
   *  Currently, the {@link Stat} class used to do this only supports Linux
   *  and FreeBSD, so the old {@link #deprecatedGetFileLinkStatusInternal(Path)}
   *  implementation (deprecated) remains further OS support is added.
   *
   * @param f File to stat
   * @param dereference whether to dereference symlinks
   * @return FileStatus of f
   * @throws IOException
   */
  private FileStatus getNativeFileLinkStatus(final Path f,
      boolean dereference) throws IOException {
    checkPath(f);
    Stat stat = new Stat(f, getDefaultBlockSize(f), dereference, this);
    FileStatus status = stat.getFileStatus();
    return status;
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // return an unqualified symlink target
    return fi.getSymlink();
  }
}
