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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Progressable;

/**
 * This is an implementation of the Hadoop Archive 
 * Filesystem. This archive Filesystem has index files
 * of the form _index* and has contents of the form
 * part-*. The index files store the indexes of the 
 * real files. The index files are of the form _masterindex
 * and _index. The master index is a level of indirection 
 * in to the index file to make the look ups faster. the index
 * file is sorted with hash code of the paths that it contains 
 * and the master index contains pointers to the positions in 
 * index for ranges of hashcodes.
 */

public class HarFileSystem extends FilterFileSystem {

  private static final Log LOG = LogFactory.getLog(HarFileSystem.class);

  public static final int VERSION = 3;

  private static final Map<URI, HarMetaData> harMetaCache =
      new ConcurrentHashMap<URI, HarMetaData>();

  // uri representation of this Har filesystem
  private URI uri;
  // the top level path of the archive
  // in the underlying file system
  private Path archivePath;
  // the har auth
  private String harAuth;

  // pointer into the static metadata cache
  private HarMetaData metadata;

  /**
   * public construction of harfilesystem
   *
   */
  public HarFileSystem() {
  }
  
  /**
   * Constructor to create a HarFileSystem with an
   * underlying filesystem.
   * @param fs
   */
  public HarFileSystem(FileSystem fs) {
    super(fs);
  }
  
  /**
   * Initialize a Har filesystem per har archive. The 
   * archive home directory is the top level directory
   * in the filesystem that contains the HAR archive.
   * Be careful with this method, you do not want to go 
   * on creating new Filesystem instances per call to 
   * path.getFileSystem().
   * the uri of Har is 
   * har://underlyingfsscheme-host:port/archivepath.
   * or 
   * har:///archivepath. This assumes the underlying filesystem
   * to be used in case not specified.
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    // decode the name
    URI underLyingURI = decodeHarURI(name, conf);
    // we got the right har Path- now check if this is 
    // truly a har filesystem
    Path harPath = archivePath(
      new Path(name.getScheme(), name.getAuthority(), name.getPath()));
    if (harPath == null) { 
      throw new IOException("Invalid path for the Har Filesystem. " + 
                           name.toString());
    }
    if (fs == null) {
      fs = FileSystem.get(underLyingURI, conf);
    }
    uri = harPath.toUri();
    archivePath = new Path(uri.getPath());
    harAuth = getHarAuth(underLyingURI);
    //check for the underlying fs containing
    // the index file
    Path masterIndexPath = new Path(archivePath, "_masterindex");
    Path archiveIndexPath = new Path(archivePath, "_index");
    if (!fs.exists(masterIndexPath) || !fs.exists(archiveIndexPath)) {
      throw new IOException("Invalid path for the Har Filesystem. " +
          "No index file in " + harPath);
    }

    metadata = harMetaCache.get(uri);
    if (metadata != null) {
      FileStatus mStat = fs.getFileStatus(masterIndexPath);
      FileStatus aStat = fs.getFileStatus(archiveIndexPath);
      if (mStat.getModificationTime() != metadata.getMasterIndexTimestamp() ||
          aStat.getModificationTime() != metadata.getArchiveIndexTimestamp()) {
        // the archive has been overwritten since we last read it
        // remove the entry from the meta data cache
        metadata = null;
        harMetaCache.remove(uri);
      }
    }
    if (metadata == null) {
      metadata = new HarMetaData(fs, masterIndexPath, archiveIndexPath);
      metadata.parseMetaData();
      harMetaCache.put(uri, metadata);
    }
  }

  // get the version of the filesystem from the masterindex file
  // the version is currently not useful since its the first version
  // of archives
  public int getHarVersion() throws IOException {
    if (metadata != null) {
      return metadata.getVersion();
    }
    else {
      throw new IOException("Invalid meta data for the Har Filesystem");
    }
  }

  /*
   * find the parent path that is the 
   * archive path in the path. The last
   * path segment that ends with .har is 
   * the path that will be returned.
   */
  private Path archivePath(Path p) {
    Path retPath = null;
    Path tmp = p;
    for (int i=0; i< p.depth(); i++) {
      if (tmp.toString().endsWith(".har")) {
        retPath = tmp;
        break;
      }
      tmp = tmp.getParent();
    }
    return retPath;
  }

  /**
   * decode the raw URI to get the underlying URI
   * @param rawURI raw Har URI
   * @return filtered URI of the underlying fileSystem
   */
  private URI decodeHarURI(URI rawURI, Configuration conf) throws IOException {
    String tmpAuth = rawURI.getAuthority();
    //we are using the default file
    //system in the config 
    //so create a underlying uri and 
    //return it
    if (tmpAuth == null) {
      //create a path 
      return FileSystem.getDefaultUri(conf);
    }
    String host = rawURI.getHost();
    if (host == null) {
      throw new IOException("URI: " + rawURI
          + " is an invalid Har URI since host==null."
          + "  Expecting har://<scheme>-<host>/<path>.");
    }
    int i = host.indexOf('-');
    if (i < 0) {
      throw new IOException("URI: " + rawURI
          + " is an invalid Har URI since '-' not found."
          + "  Expecting har://<scheme>-<host>/<path>.");
    }
    final String underLyingScheme = host.substring(0, i);
    i++;
    final String underLyingHost = i == host.length()? null: host.substring(i);
    int underLyingPort = rawURI.getPort();
    String auth = (underLyingHost == null && underLyingPort == -1)?
                  null:(underLyingHost+":"+underLyingPort);
    URI tmp = null;
    if (rawURI.getQuery() != null) {
      // query component not allowed
      throw new IOException("query component in Path not supported  " + rawURI);
    }
    try {
      tmp = new URI(underLyingScheme, auth, rawURI.getPath(), 
            rawURI.getQuery(), rawURI.getFragment());
    } catch (URISyntaxException e) {
        // do nothing should not happen
    }
    return tmp;
  }

  private static String decodeString(String str)
    throws UnsupportedEncodingException {
    return URLDecoder.decode(str, "UTF-8");
  }

  private String decodeFileName(String fname) 
    throws UnsupportedEncodingException {
    int version = metadata.getVersion();
    if (version == 2 || version == 3){
      return decodeString(fname);
    }
    return fname;
  }

  /**
   * return the top level archive.
   */
  public Path getWorkingDirectory() {
    return new Path(uri.toString());
  }
  
  /**
   * Create a har specific auth 
   * har-underlyingfs:port
   * @param underLyingURI the uri of underlying
   * filesystem
   * @return har specific auth
   */
  private String getHarAuth(URI underLyingUri) {
    String auth = underLyingUri.getScheme() + "-";
    if (underLyingUri.getHost() != null) {
      auth += underLyingUri.getHost() + ":";
      if (underLyingUri.getPort() != -1) {
        auth +=  underLyingUri.getPort();
      }
    }
    else {
      auth += ":";
    }
    return auth;
  }
  
  /**
   * Returns the uri of this filesystem.
   * The uri is of the form 
   * har://underlyingfsschema-host:port/pathintheunderlyingfs
   */
  @Override
  public URI getUri() {
    return this.uri;
  }
  
  /**
   * this method returns the path 
   * inside the har filesystem.
   * this is relative path inside 
   * the har filesystem.
   * @param path the fully qualified path in the har filesystem.
   * @return relative path in the filesystem.
   */
  private Path getPathInHar(Path path) {
    Path harPath = new Path(path.toUri().getPath());
    if (archivePath.compareTo(harPath) == 0)
      return new Path(Path.SEPARATOR);
    Path tmp = new Path(harPath.getName());
    Path parent = harPath.getParent();
    while (!(parent.compareTo(archivePath) == 0)) {
      if (parent.toString().equals(Path.SEPARATOR)) {
        tmp = null;
        break;
      }
      tmp = new Path(parent.getName(), tmp);
      parent = parent.getParent();
    }
    if (tmp != null) 
      tmp = new Path(Path.SEPARATOR, tmp);
    return tmp;
  }
  
  //the relative path of p. basically 
  // getting rid of /. Parsing and doing 
  // string manipulation is not good - so
  // just use the path api to do it.
  private Path makeRelative(String initial, Path p) {
    String scheme = this.uri.getScheme();
    String authority = this.uri.getAuthority();
    Path root = new Path(Path.SEPARATOR);
    if (root.compareTo(p) == 0)
      return new Path(scheme, authority, initial);
    Path retPath = new Path(p.getName());
    Path parent = p.getParent();
    for (int i=0; i < p.depth()-1; i++) {
      retPath = new Path(parent.getName(), retPath);
      parent = parent.getParent();
    }
    return new Path(new Path(scheme, authority, initial),
      retPath.toString());
  }
  
  /* this makes a path qualified in the har filesystem
   * (non-Javadoc)
   * @see org.apache.hadoop.fs.FilterFileSystem#makeQualified(
   * org.apache.hadoop.fs.Path)
   */
  @Override
  public Path makeQualified(Path path) {
    // make sure that we just get the 
    // path component 
    Path fsPath = path;
    if (!path.isAbsolute()) {
      fsPath = new Path(archivePath, path);
    }

    URI tmpURI = fsPath.toUri();
    //change this to Har uri 
    return new Path(uri.getScheme(), harAuth, tmpURI.getPath());
  }

  /**
   * Fix offset and length of block locations.
   * Note that this method modifies the original array.
   * @param locations block locations of har part file
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @param fileOffsetInHar the offset of the desired file in the har part file
   * @return block locations with fixed offset and length
   */  
  static BlockLocation[] fixBlockLocations(BlockLocation[] locations,
                                          long start,
                                          long len,
                                          long fileOffsetInHar) {
    // offset 1 past last byte of desired range
    long end = start + len;

    for (BlockLocation location : locations) {
      // offset of part block relative to beginning of desired file
      // (may be negative if file starts in this part block)
      long harBlockStart = location.getOffset() - fileOffsetInHar;
      // offset 1 past last byte of har block relative to beginning of
      // desired file
      long harBlockEnd = harBlockStart + location.getLength();
      
      if (start > harBlockStart) {
        // desired range starts after beginning of this har block
        // fix offset to beginning of relevant range (relative to desired file)
        location.setOffset(start);
        // fix length to relevant portion of har block
        location.setLength(location.getLength() - (start - harBlockStart));
      } else {
        // desired range includes beginning of this har block
        location.setOffset(harBlockStart);
      }
      
      if (harBlockEnd > end) {
        // range ends before end of this har block
        // fix length to remove irrelevant portion at the end
        location.setLength(location.getLength() - (harBlockEnd - end));
      }
    }
    
    return locations;
  }
  
  /**
   * Get block locations from the underlying fs and fix their
   * offsets and lengths.
   * @param file the input filestatus to get block locations
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @return block locations for this segment of file
   * @throws IOException
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
                                               long len) throws IOException {
    HarStatus hstatus = getFileHarStatus(file.getPath());
    Path partPath = new Path(archivePath, hstatus.getPartName());
    FileStatus partStatus = metadata.getPartFileStatus(partPath);

    // get all part blocks that overlap with the desired file blocks
    BlockLocation[] locations = 
      fs.getFileBlockLocations(partStatus,
                               hstatus.getStartIndex() + start, len);

    return fixBlockLocations(locations, start, len, hstatus.getStartIndex());
  }
  
  /**
   * the hash of the path p inside iniside
   * the filesystem
   * @param p the path in the harfilesystem
   * @return the hash code of the path.
   */
  public static int getHarHash(Path p) {
    return (p.toString().hashCode() & 0x7fffffff);
  }
  
  static class Store {
    public Store() {
      begin = end = startHash = endHash = 0;
    }
    public Store(long begin, long end, int startHash, int endHash) {
      this.begin = begin;
      this.end = end;
      this.startHash = startHash;
      this.endHash = endHash;
    }
    public long begin;
    public long end;
    public int startHash;
    public int endHash;
  }
  
  /**
   * Get filestatuses of all the children of a given directory. This just reads
   * through index file and reads line by line to get all statuses for children
   * of a directory. Its a brute force way of getting all such filestatuses
   * 
   * @param parent
   *          the parent path directory
   * @param statuses
   *          the list to add the children filestatuses to
   * @param children
   *          the string list of children for this parent
   * @param archiveIndexStat
   *          the archive index filestatus
   */
  private void fileStatusesInIndex(HarStatus parent, List<FileStatus> statuses,
      List<String> children) throws IOException {
    String parentString = parent.getName();
    if (!parentString.endsWith(Path.SEPARATOR)){
        parentString += Path.SEPARATOR;
    }
    Path harPath = new Path(parentString);
    int harlen = harPath.depth();
    final Map<String, FileStatus> cache = new TreeMap<String, FileStatus>();

    for (HarStatus hstatus : metadata.archive.values()) {
      String child = hstatus.getName();
      if ((child.startsWith(parentString))) {
        Path thisPath = new Path(child);
        if (thisPath.depth() == harlen + 1) {
          statuses.add(toFileStatus(hstatus, cache));
        }
      }
    }
  }

  /**
   * Combine the status stored in the index and the underlying status. 
   * @param h status stored in the index
   * @param cache caching the underlying file statuses
   * @return the combined file status
   * @throws IOException
   */
  private FileStatus toFileStatus(HarStatus h,
      Map<String, FileStatus> cache) throws IOException {
    FileStatus underlying = null;
    if (cache != null) {
      underlying = cache.get(h.partName);
    }
    if (underlying == null) {
      final Path p = h.isDir? archivePath: new Path(archivePath, h.partName);
      underlying = fs.getFileStatus(p);
      if (cache != null) {
        cache.put(h.partName, underlying);
      }
    }

    long modTime = 0;
    int version = metadata.getVersion();
    if (version < 3) {
      modTime = underlying.getModificationTime();
    } else if (version == 3) {
      modTime = h.getModificationTime();
    }

    return new FileStatus(
        h.isDir()? 0L: h.getLength(),
        h.isDir(),
        underlying.getReplication(),
        underlying.getBlockSize(),
        modTime,
        underlying.getAccessTime(),
        underlying.getPermission(),
        underlying.getOwner(),
        underlying.getGroup(),
        makeRelative(this.uri.getPath(), new Path(h.name)));
  }

  // a single line parser for hadoop archives status 
  // stored in a single line in the index files 
  // the format is of the form 
  // filename "dir"/"file" partFileName startIndex length 
  // <space seperated children>
  private class HarStatus {
    boolean isDir;
    String name;
    List<String> children;
    String partName;
    long startIndex;
    long length;
    long modificationTime = 0;

    public HarStatus(String harString) throws UnsupportedEncodingException {
      String[] splits = harString.split(" ");
      this.name = decodeFileName(splits[0]);
      this.isDir = "dir".equals(splits[1]) ? true: false;
      // this is equal to "none" if its a directory
      this.partName = splits[2];
      this.startIndex = Long.parseLong(splits[3]);
      this.length = Long.parseLong(splits[4]);

      int version = metadata.getVersion();
      String[] propSplits = null;
      // propSplits is used to retrieve the metainformation that Har versions
      // 1 & 2 missed (modification time, permission, owner group).
      // These fields are stored in an encoded string placed in different
      // locations depending on whether it's a file or directory entry.
      // If it's a directory, the string will be placed at the partName
      // location (directories have no partName because they don't have data
      // to be stored). This is done because the number of fields in a
      // directory entry is unbounded (all children are listed at the end)
      // If it's a file, the string will be the last field.
      if (isDir) {
        if (version == 3){
          propSplits = decodeString(this.partName).split(" ");
        }
        children = new ArrayList<String>();
        for (int i = 5; i < splits.length; i++) {
          children.add(decodeFileName(splits[i]));
        }
      } else if (version == 3) {
        propSplits = decodeString(splits[5]).split(" ");
      }

      if (propSplits != null && propSplits.length >= 4) {
        modificationTime = Long.parseLong(propSplits[0]);
        // the fields below are stored in the file but are currently not used
        // by HarFileSystem
        // permission = new FsPermission(Short.parseShort(propSplits[1]));
        // owner = decodeString(propSplits[2]);
        // group = decodeString(propSplits[3]);
      }
    }
    public boolean isDir() {
      return isDir;
    }
    
    public String getName() {
      return name;
    }
    public String getPartName() {
      return partName;
    }
    public long getStartIndex() {
      return startIndex;
    }
    public long getLength() {
      return length;
    }
    public long getModificationTime() {
      return modificationTime;
    }
  }
  
  /**
   * return the filestatus of files in har archive.
   * The permission returned are that of the archive
   * index files. The permissions are not persisted 
   * while creating a hadoop archive.
   * @param f the path in har filesystem
   * @return filestatus.
   * @throws IOException
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    HarStatus hstatus = getFileHarStatus(f);
    return toFileStatus(hstatus, null);
  }

  private HarStatus getFileHarStatus(Path f) throws IOException {
    // get the fs DataInputStream for the underlying file
    // look up the index.
    Path p = makeQualified(f);
    Path harPath = getPathInHar(p);
    if (harPath == null) {
      throw new IOException("Invalid file name: " + f + " in " + uri);
    }
    HarStatus hstatus = metadata.archive.get(harPath);
    if (hstatus == null) {
      throw new FileNotFoundException("File: " +  f + " does not exist in " + uri);
    }
    return hstatus;
  }

  /**
   * @return null since no checksum algorithm is implemented.
   */
  public FileChecksum getFileChecksum(Path f) {
    return null;
  }

  /**
   * Returns a har input stream which fakes end of 
   * file. It reads the index files to get the part 
   * file name and the size and start of the file.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // get the fs DataInputStream for the underlying file
    HarStatus hstatus = getFileHarStatus(f);
    // we got it.. woo hooo!!! 
    if (hstatus.isDir()) {
      throw new FileNotFoundException(f + " : not a file in " +
                archivePath);
    }
    return new HarFSDataInputStream(fs, new Path(archivePath, 
        hstatus.getPartName()),
        hstatus.getStartIndex(), hstatus.getLength(), bufferSize);
  }
 
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    throw new IOException("Har: create not allowed.");
  }
  
  @Override
  public void close() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch(IOException ie) {
        //this might already be closed
        // ignore
      }
    }
  }
  
  /**
   * Not implemented.
   */
  @Override
  public boolean setReplication(Path src, short replication) throws IOException{
    throw new IOException("Har: setreplication not allowed");
  }
  
  /**
   * Not implemented.
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException { 
    throw new IOException("Har: delete not allowed");
  }
  
  /**
   * liststatus returns the children of a directory 
   * after looking up the index files.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    //need to see if the file is an index in file
    //get the filestatus of the archive directory
    // we will create fake filestatuses to return
    // to the client
    List<FileStatus> statuses = new ArrayList<FileStatus>();
    Path tmpPath = makeQualified(f);
    Path harPath = getPathInHar(tmpPath);
    HarStatus hstatus = metadata.archive.get(harPath);
    if (hstatus == null) {
      throw new FileNotFoundException("File " + f + " not found in " + archivePath);
    }
    if (hstatus.isDir()) {
      fileStatusesInIndex(hstatus, statuses, hstatus.children);
    } else {
      statuses.add(toFileStatus(hstatus, null));
    }
    
    return statuses.toArray(new FileStatus[statuses.size()]);
  }
  
  /**
   * return the top level archive path.
   */
  public Path getHomeDirectory() {
    return new Path(uri.toString());
  }
  
  public void setWorkingDirectory(Path newDir) {
    //does nothing.
  }
  
  /**
   * not implemented.
   */
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new IOException("Har: mkdirs not allowed");
  }
  
  /**
   * not implemented.
   */
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws 
        IOException {
    throw new IOException("Har: copyfromlocalfile not allowed");
  }
  
  /**
   * copies the file in the har filesystem to a local file.
   */
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) 
    throws IOException {
    FileUtil.copy(this, src, getLocal(getConf()), dst, false, getConf());
  }
  
  /**
   * not implemented.
   */
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) 
    throws IOException {
    throw new IOException("Har: startLocalOutput not allowed");
  }
  
  /**
   * not implemented.
   */
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) 
    throws IOException {
    throw new IOException("Har: completeLocalOutput not allowed");
  }
  
  /**
   * not implemented.
   */
  public void setOwner(Path p, String username, String groupname)
    throws IOException {
    throw new IOException("Har: setowner not allowed");
  }

  /**
   * Not implemented.
   */
  public void setPermission(Path p, FsPermission permisssion) 
    throws IOException {
    throw new IOException("Har: setPermission not allowed");
  }
  
  /**
   * Hadoop archives input stream. This input stream fakes EOF 
   * since archive files are part of bigger part files.
   */
  private static class HarFSDataInputStream extends FSDataInputStream {
    /**
     * Create an input stream that fakes all the reads/positions/seeking.
     */
    private static class HarFsInputStream extends FSInputStream {
      private long position, start, end;
      //The underlying data input stream that the
      // underlying filesystem will return.
      private FSDataInputStream underLyingStream;
      //one byte buffer
      private byte[] oneBytebuff = new byte[1];
      HarFsInputStream(FileSystem fs, Path path, long start,
          long length, int bufferSize) throws IOException {
        underLyingStream = fs.open(path, bufferSize);
        underLyingStream.seek(start);
        // the start of this file in the part file
        this.start = start;
        // the position pointer in the part file
        this.position = start;
        // the end pointer in the part file
        this.end = start + length;
      }
      
      public synchronized int available() throws IOException {
        long remaining = end - underLyingStream.getPos();
        if (remaining > (long)Integer.MAX_VALUE) {
          return Integer.MAX_VALUE;
        }
        return (int) remaining;
      }
      
      public synchronized  void close() throws IOException {
        underLyingStream.close();
        super.close();
      }
      
      //not implemented
      @Override
      public void mark(int readLimit) {
        // do nothing 
      }
      
      /**
       * reset is not implemented
       */
      public void reset() throws IOException {
        throw new IOException("reset not implemented.");
      }
      
      public synchronized int read() throws IOException {
        int ret = read(oneBytebuff, 0, 1);
        return (ret <= 0) ? -1: (oneBytebuff[0] & 0xff);
      }
      
      public synchronized int read(byte[] b) throws IOException {
        int ret = read(b, 0, b.length);
        if (ret != -1) {
          position += ret;
        }
        return ret;
      }
      
      /**
       * 
       */
      public synchronized int read(byte[] b, int offset, int len) 
        throws IOException {
        int newlen = len;
        int ret = -1;
        if (position + len > end) {
          newlen = (int) (end - position);
        }
        // end case
        if (newlen == 0) 
          return ret;
        ret = underLyingStream.read(b, offset, newlen);
        position += ret;
        return ret;
      }
      
      public synchronized long skip(long n) throws IOException {
        long tmpN = n;
        if (tmpN > 0) {
          if (position + tmpN > end) {
            tmpN = end - position;
          }
          underLyingStream.seek(tmpN + position);
          position += tmpN;
          return tmpN;
        }
        return (tmpN < 0)? -1 : 0;
      }
      
      public synchronized long getPos() throws IOException {
        return (position - start);
      }
      
      public synchronized void seek(long pos) throws IOException {
        if (pos < 0 || (start + pos > end)) {
          throw new IOException("Failed to seek: EOF");
        }
        position = start + pos;
        underLyingStream.seek(position);
      }

      public boolean seekToNewSource(long targetPos) throws IOException {
        //do not need to implement this
        // hdfs in itself does seektonewsource 
        // while reading.
        return false;
      }
      
      /**
       * implementing position readable. 
       */
      public int read(long pos, byte[] b, int offset, int length) 
      throws IOException {
        int nlength = length;
        if (start + nlength + pos > end) {
          nlength = (int) (end - (start + pos));
        }
        return underLyingStream.read(pos + start , b, offset, nlength);
      }
      
      /**
       * position readable again.
       */
      public void readFully(long pos, byte[] b, int offset, int length) 
      throws IOException {
        if (start + length + pos > end) {
          throw new IOException("Not enough bytes to read.");
        }
        underLyingStream.readFully(pos + start, b, offset, length);
      }
      
      public void readFully(long pos, byte[] b) throws IOException {
          readFully(pos, b, 0, b.length);
      }
      
    }
  
    /**
     * constructors for har input stream.
     * @param fs the underlying filesystem
     * @param p The path in the underlying filesystem
     * @param start the start position in the part file
     * @param length the length of valid data in the part file
     * @param bufsize the buffer size
     * @throws IOException
     */
    public HarFSDataInputStream(FileSystem fs, Path  p, long start, 
        long length, int bufsize) throws IOException {
        super(new HarFsInputStream(fs, p, start, length, bufsize));
    }

    /**
     * constructor for har input stream.
     * @param fs the underlying filesystem
     * @param p the path in the underlying file system
     * @param start the start position in the part file
     * @param length the length of valid data in the part file.
     * @throws IOException
     */
    public HarFSDataInputStream(FileSystem fs, Path  p, long start, long length)
      throws IOException {
        super(new HarFsInputStream(fs, p, start, length, 0));
    }
  }

  private class HarMetaData {
    private FileSystem fs;
    private int version;
    // the masterIndex of the archive
    private Path masterIndexPath;
    // the index file 
    private Path archiveIndexPath;

    private long masterIndexTimestamp;
    private long archiveIndexTimestamp;

    List<Store> stores = new ArrayList<Store>();
    Map<Path, HarStatus> archive = new HashMap<Path, HarStatus>();
    private Map<Path, FileStatus> partFileStatuses = new HashMap<Path, FileStatus>();

    public HarMetaData(FileSystem fs, Path masterIndexPath, Path archiveIndexPath) {
      this.fs = fs;
      this.masterIndexPath = masterIndexPath;
      this.archiveIndexPath = archiveIndexPath;
    }

    public FileStatus getPartFileStatus(Path partPath) throws IOException {
      FileStatus status;
      status = partFileStatuses.get(partPath);
      if (status == null) {
        status = fs.getFileStatus(partPath);
        partFileStatuses.put(partPath, status);
      }
      return status;
    }

    public long getMasterIndexTimestamp() {
      return masterIndexTimestamp;
    }

    public long getArchiveIndexTimestamp() {
      return archiveIndexTimestamp;
    }

    private int getVersion() {
      return version;
    }

    private void parseMetaData() throws IOException {
      Text line;
      long read;
      FSDataInputStream in = null;
      LineReader lin = null;

      try {
        in = fs.open(masterIndexPath);
        FileStatus masterStat = fs.getFileStatus(masterIndexPath);
        masterIndexTimestamp = masterStat.getModificationTime();
        lin = new LineReader(in, getConf());
        line = new Text();
        read = lin.readLine(line);

        // the first line contains the version of the index file
        String versionLine = line.toString();
        String[] arr = versionLine.split(" ");
        version = Integer.parseInt(arr[0]);
        // make it always backwards-compatible
        if (this.version > HarFileSystem.VERSION) {
          throw new IOException("Invalid version " + 
              this.version + " expected " + HarFileSystem.VERSION);
        }

        // each line contains a hashcode range and the index file name
        String[] readStr = null;
        while(read < masterStat.getLen()) {
          int b = lin.readLine(line);
          read += b;
          readStr = line.toString().split(" ");
          int startHash = Integer.parseInt(readStr[0]);
          int endHash  = Integer.parseInt(readStr[1]);
          stores.add(new Store(Long.parseLong(readStr[2]), 
              Long.parseLong(readStr[3]), startHash,
              endHash));
          line.clear();
        }
      } finally {
        IOUtils.cleanup(LOG, lin, in);
      }

      FSDataInputStream aIn = fs.open(archiveIndexPath);
      try {
        FileStatus archiveStat = fs.getFileStatus(archiveIndexPath);
        archiveIndexTimestamp = archiveStat.getModificationTime();
        LineReader aLin;

        // now start reading the real index file
        for (Store s: stores) {
          read = 0;
          aIn.seek(s.begin);
          aLin = new LineReader(aIn, getConf());
          while (read + s.begin < s.end) {
            int tmp = aLin.readLine(line);
            read += tmp;
            String lineFeed = line.toString();
            String[] parsed = lineFeed.split(" ");
            parsed[0] = decodeFileName(parsed[0]);
            archive.put(new Path(parsed[0]), new HarStatus(lineFeed));
            line.clear();
          }
        }
      } finally {
        IOUtils.cleanup(LOG, aIn);
      }
    }
  }
  
  /*
   * testing purposes only:
   */
  HarMetaData getMetadata() {
    return metadata;
  }
}
