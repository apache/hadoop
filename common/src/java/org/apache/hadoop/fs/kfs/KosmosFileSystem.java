/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * 
 * Implements the Hadoop FS interfaces to allow applications to store
 *files in Kosmos File System (KFS).
 */

package org.apache.hadoop.fs.kfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A FileSystem backed by KFS.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KosmosFileSystem extends FileSystem {

    private FileSystem localFs;
    private IFSImpl kfsImpl = null;
    private URI uri;
    private Path workingDir = new Path("/");

    public KosmosFileSystem() {

    }

    KosmosFileSystem(IFSImpl fsimpl) {
        this.kfsImpl = fsimpl;
    }

    @Override
    public URI getUri() {
	return uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
      try {
        if (kfsImpl == null) {
          if (uri.getHost() == null) {
            kfsImpl = new KFSImpl(conf.get("fs.kfs.metaServerHost", ""),
                                  conf.getInt("fs.kfs.metaServerPort", -1),
                                  statistics);
          } else {
            kfsImpl = new KFSImpl(uri.getHost(), uri.getPort(), statistics);
          }
        }

        this.localFs = FileSystem.getLocal(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/user", System.getProperty("user.name")
                                   ).makeQualified(this);
        setConf(conf);

      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Unable to initialize KFS");
        System.exit(-1);
      }
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
    public boolean mkdirs(Path path, FsPermission permission
        ) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

	int res;

	// System.out.println("Calling mkdirs on: " + srep);

	res = kfsImpl.mkdirs(srep);
	
	return res == 0;
    }

    @Override
    public boolean isDirectory(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

	// System.out.println("Calling isdir on: " + srep);

        return kfsImpl.isDirectory(srep);
    }

    @Override
    public boolean isFile(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        return kfsImpl.isFile(srep);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

        if(!kfsImpl.exists(srep))
          throw new FileNotFoundException("File " + path + " does not exist.");

        if (kfsImpl.isFile(srep))
                return new FileStatus[] { getFileStatus(path) } ;

        return kfsImpl.readdirplus(absolute);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        if (!kfsImpl.exists(srep)) {
          throw new FileNotFoundException("File " + path + " does not exist.");
        }
        if (kfsImpl.isDirectory(srep)) {
            // System.out.println("Status of path: " + path + " is dir");
            return new FileStatus(0, true, 1, 0, kfsImpl.getModificationTime(srep), 
                                  path.makeQualified(this));
        } else {
            // System.out.println("Status of path: " + path + " is file");
            return new FileStatus(kfsImpl.filesize(srep), false, 
                                  kfsImpl.getReplication(srep),
                                  getDefaultBlockSize(),
                                  kfsImpl.getModificationTime(srep),
                                  path.makeQualified(this));
        }
    }
    
    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
        Path parent = f.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }

        Path absolute = makeAbsolute(f);
        String srep = absolute.toUri().getPath();

        return kfsImpl.append(srep, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path file, FsPermission permission,
                                     boolean overwrite, int bufferSize,
				     short replication, long blockSize, Progressable progress)
	throws IOException {

        if (exists(file)) {
            if (overwrite) {
                delete(file, true);
            } else {
                throw new IOException("File already exists: " + file);
            }
        }

	Path parent = file.getParent();
	if (parent != null && !mkdirs(parent)) {
	    throw new IOException("Mkdirs failed to create " + parent);
	}

        Path absolute = makeAbsolute(file);
        String srep = absolute.toUri().getPath();

        return kfsImpl.create(srep, replication, bufferSize, progress);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        if (!exists(path))
            throw new IOException("File does not exist: " + path);

        Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

        return kfsImpl.open(srep, bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
	Path absoluteS = makeAbsolute(src);
        String srepS = absoluteS.toUri().getPath();
	Path absoluteD = makeAbsolute(dst);
        String srepD = absoluteD.toUri().getPath();

        // System.out.println("Calling rename on: " + srepS + " -> " + srepD);

        return kfsImpl.rename(srepS, srepD) == 0;
    }

    // recursively delete the directory and its contents
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
      Path absolute = makeAbsolute(path);
      String srep = absolute.toUri().getPath();
      if (kfsImpl.isFile(srep))
        return kfsImpl.remove(srep) == 0;

      FileStatus[] dirEntries = listStatus(absolute);
      if (!recursive && (dirEntries.length != 0)) {
        throw new IOException("Directory " + path.toString() + 
        " is not empty.");
      }

      for (int i = 0; i < dirEntries.length; i++) {
        delete(new Path(absolute, dirEntries[i].getPath()), recursive);
      }
      return kfsImpl.rmdir(srep) == 0;
    }
    
    @Override
    public short getDefaultReplication() {
	return 3;
    }

    @Override
    public boolean setReplication(Path path, short replication)
	throws IOException {

	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

        int res = kfsImpl.setReplication(srep, replication);
        return res >= 0;
    }

    // 64MB is the KFS block size

    @Override
    public long getDefaultBlockSize() {
	return 1 << 26;
    }

    @Deprecated            
    public void lock(Path path, boolean shared) throws IOException {

    }

    @Deprecated            
    public void release(Path path) throws IOException {

    }

    /**
     * Return null if the file doesn't exist; otherwise, get the
     * locations of the various chunks of the file file from KFS.
     */
    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
        long len) throws IOException {

      if (file == null) {
        return null;
      }
      String srep = makeAbsolute(file.getPath()).toUri().getPath();
      String[][] hints = kfsImpl.getDataLocation(srep, start, len);
      if (hints == null) {
        return null;
      }
      BlockLocation[] result = new BlockLocation[hints.length];
      long blockSize = getDefaultBlockSize();
      long length = len;
      long blockStart = start;
      for(int i=0; i < result.length; ++i) {
        result[i] = new BlockLocation(null, hints[i], blockStart, 
                                      length < blockSize ? length : blockSize);
        blockStart += blockSize;
        length -= blockSize;
      }
      return result;
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
}
