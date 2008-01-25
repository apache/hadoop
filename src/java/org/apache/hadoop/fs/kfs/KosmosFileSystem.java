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
 * @author: Sriram Rao (Kosmix Corp.)
 * 
 * Implements the Hadoop FS interfaces to allow applications to store
 *files in Kosmos File System (KFS).
 */

package org.apache.hadoop.fs.kfs;

import java.io.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A FileSystem backed by KFS.
 *
 */

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

    public URI getUri() {
	return uri;
    }

    public void initialize(URI uri, Configuration conf) throws IOException {
        
        try {
            if (kfsImpl == null) {
                kfsImpl = new KFSImpl(conf.get("fs.kfs.metaServerHost", ""),
                                      conf.getInt("fs.kfs.metaServerPort", -1));
            }
            this.localFs = FileSystem.getLocal(conf);
            this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
            
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unable to initialize KFS");
            System.exit(-1);
        }
    }

    @Deprecated
    public String getName() {
	return getUri().toString();
    }

    public Path getWorkingDirectory() {
	return workingDir;
    }

    public void setWorkingDirectory(Path dir) {
	workingDir = makeAbsolute(dir);
    }

    private Path makeAbsolute(Path path) {
	if (path.isAbsolute()) {
	    return path;
	}
	return new Path(workingDir, path);
    }

    public boolean exists(Path path) throws IOException {
	// stat the path to make sure it exists
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        return kfsImpl.exists(srep);
    }

    public boolean mkdirs(Path path, FsPermission permission
        ) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

	int res;

	// System.out.println("Calling mkdirs on: " + srep);

	res = kfsImpl.mkdirs(srep);
	
	return res == 0;
    }

    @Deprecated
    public boolean isDirectory(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

	// System.out.println("Calling isdir on: " + srep);

        return kfsImpl.isDirectory(srep);
    }

    @Deprecated
    public boolean isFile(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        return kfsImpl.isFile(srep);
    }

    public long getContentLength(Path path)  throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

	if (kfsImpl.isFile(srep))
	    return kfsImpl.filesize(srep);
        
	String[] entries = kfsImpl.readdir(srep);

        if (entries == null)
            return 0;

        // kfsreaddir() returns "." and ".."; strip them before
        // passing back to hadoop fs.
	long numEntries = 0;
	for (int i = 0; i < entries.length; i++) {
	    if ((entries[i].compareTo(".") == 0) || (entries[i].compareTo("..") == 0))
		continue;
	    numEntries++;
	}
        return numEntries;
    }

    public FileStatus[] listStatus(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

	if (kfsImpl.isFile(srep))
	    return new FileStatus[] { getFileStatus(path) } ;

	String[] entries = kfsImpl.readdir(srep);

        if (entries == null)
            return null;

        // kfsreaddir() returns "." and ".."; strip them before
        // passing back to hadoop fs.
	int numEntries = 0;
	for (int i = 0; i < entries.length; i++) {
	    if ((entries[i].compareTo(".") == 0) || (entries[i].compareTo("..") == 0))
		continue;
	    numEntries++;
	}
	if (numEntries == 0) {
	    return null;
        }

        // System.out.println("Calling listStatus on: " + path);

	FileStatus[] pathEntries = new FileStatus[numEntries];
	int j = 0;
	for (int i = 0; i < entries.length; i++) {
	    if ((entries[i].compareTo(".") == 0) || (entries[i].compareTo("..") == 0))
		continue;

	    pathEntries[j] = getFileStatus(new Path(path, entries[i]));
	    j++;
	}
	return pathEntries;
    }

    public FileStatus getFileStatus(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        if (!kfsImpl.exists(srep)) {
          throw new FileNotFoundException("File " + path + " does not exist.");
        }
        if (kfsImpl.isDirectory(srep)) {
            // System.out.println("Status of path: " + path + " is dir");
            return new FileStatus(0, true, 1, 0, 0, path);
        } else {
            // System.out.println("Status of path: " + path + " is file");
            return new FileStatus(kfsImpl.filesize(srep), false, 
                                  kfsImpl.getReplication(srep),
                                  getDefaultBlockSize(),
                                  kfsImpl.getModificationTime(srep), path);
        }
    }

    
    public Path[] listPaths(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

	if (kfsImpl.isFile(srep))
	    return new Path[] { path } ;

	String[] entries = kfsImpl.readdir(srep);

        if (entries == null)
            return null;

        // kfsreaddir() returns "." and ".."; strip them before
        // passing back to hadoop fs.
	int numEntries = 0;
	for (int i = 0; i < entries.length; i++) {
	    if ((entries[i].compareTo(".") == 0) || (entries[i].compareTo("..") == 0))
		continue;
	    numEntries++;
	}
	if (numEntries == 0) {
	    return null;
        }
	Path[] pathEntries = new Path[numEntries];
	int j = 0;
	for (int i = 0; i < entries.length; i++) {
	    if ((entries[i].compareTo(".") == 0) || (entries[i].compareTo("..") == 0))
		continue;

	    pathEntries[j] = new Path(path, entries[i]);
	    j++;
	}
	return pathEntries;

    }

    public FSDataOutputStream create(Path file, FsPermission permission,
                                     boolean overwrite, int bufferSize,
				     short replication, long blockSize, Progressable progress)
	throws IOException {

        if (exists(file)) {
            if (overwrite) {
                delete(file);
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

        return kfsImpl.create(srep, replication, bufferSize);
    }

    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        if (!exists(path))
            throw new IOException("File does not exist: " + path);

        Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

        return kfsImpl.open(srep, bufferSize);
    }

    public boolean rename(Path src, Path dst) throws IOException {
	Path absoluteS = makeAbsolute(src);
        String srepS = absoluteS.toUri().getPath();
	Path absoluteD = makeAbsolute(dst);
        String srepD = absoluteD.toUri().getPath();

        // System.out.println("Calling rename on: " + srepS + " -> " + srepD);

        return kfsImpl.rename(srepS, srepD) == 0;
    }

    // recursively delete the directory and its contents
    public boolean delete(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

        if (kfsImpl.isFile(srep))
            return kfsImpl.remove(srep) == 0;

        Path[] dirEntries = listPaths(absolute);
        if (dirEntries != null) {
            for (int i = 0; i < dirEntries.length; i++) {
                delete(new Path(absolute, dirEntries[i]));
            }
        }
        return kfsImpl.rmdir(srep) == 0;
    }

    @Deprecated
    public long getLength(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        return kfsImpl.filesize(srep);
    }

    @Deprecated
    public short getReplication(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        return kfsImpl.getReplication(srep);
    }

    public short getDefaultReplication() {
	return 3;
    }

    public boolean setReplication(Path path, short replication)
	throws IOException {

	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();

        int res = kfsImpl.setReplication(srep, replication);
        return res >= 0;
    }

    // 64MB is the KFS block size

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
    public String[][] getFileCacheHints(Path f, long start, long len)
	throws IOException {
	if (!exists(f)) {
	    return null;
	}
        String srep = makeAbsolute(f).toUri().getPath();
        String[][] hints = kfsImpl.getDataLocation(srep, start, len);
        return hints;
    }

    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
	FileUtil.copy(localFs, src, this, dst, delSrc, getConf());
    }

    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
	FileUtil.copy(this, src, localFs, dst, delSrc, getConf());
    }

    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
	throws IOException {
	return tmpLocalFile;
    }

    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
	throws IOException {
	moveFromLocalFile(tmpLocalFile, fsOutputFile);
    }
}
