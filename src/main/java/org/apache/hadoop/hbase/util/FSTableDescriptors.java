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
package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;


/**
 * Implementation of {@link TableDescriptors} that reads descriptors from the
 * passed filesystem.  It expects descriptors to be in a file under the
 * table's directory in FS.  Can be read-only -- i.e. does not modify
 * the filesystem or can be read and write.
 * 
 * <p>Also has utility for keeping up the table descriptors tableinfo file.
 * The table schema file is kept under the table directory in the filesystem.
 * It has a {@link #TABLEINFO_NAME} prefix and then a suffix that is the
 * edit sequenceid: e.g. <code>.tableinfo.0000000003</code>.  This sequenceid
 * is always increasing.  It starts at zero.  The table schema file with the
 * highest sequenceid has the most recent schema edit. Usually there is one file
 * only, the most recent but there may be short periods where there are more
 * than one file. Old files are eventually cleaned.  Presumption is that there
 * will not be lots of concurrent clients making table schema edits.  If so,
 * the below needs a bit of a reworking and perhaps some supporting api in hdfs.
 */
public class FSTableDescriptors implements TableDescriptors {
  private static final Log LOG = LogFactory.getLog(FSTableDescriptors.class);
  private final FileSystem fs;
  private final Path rootdir;
  private final boolean fsreadonly;
  long cachehits = 0;
  long invocations = 0;

  /** The file name used to store HTD in HDFS  */
  public static final String TABLEINFO_NAME = ".tableinfo";

  // This cache does not age out the old stuff.  Thinking is that the amount
  // of data we keep up in here is so small, no need to do occasional purge.
  // TODO.
  private final Map<String, TableDescriptorModtime> cache =
    new ConcurrentHashMap<String, TableDescriptorModtime>();

  /**
   * Data structure to hold modification time and table descriptor.
   */
  static class TableDescriptorModtime {
    private final HTableDescriptor descriptor;
    private final long modtime;

    TableDescriptorModtime(final long modtime, final HTableDescriptor htd) {
      this.descriptor = htd;
      this.modtime = modtime;
    }

    long getModtime() {
      return this.modtime;
    }

    HTableDescriptor getTableDescriptor() {
      return this.descriptor;
    }
  }

  public FSTableDescriptors(final FileSystem fs, final Path rootdir) {
    this(fs, rootdir, false);
  }

  /**
   * @param fs
   * @param rootdir
   * @param fsreadOnly True if we are read-only when it comes to filesystem
   * operations; i.e. on remove, we do not do delete in fs.
   */
  public FSTableDescriptors(final FileSystem fs, final Path rootdir,
      final boolean fsreadOnly) {
    super();
    this.fs = fs;
    this.rootdir = rootdir;
    this.fsreadonly = fsreadOnly;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.TableDescriptors#getHTableDescriptor(java.lang.String)
   */
  @Override
  public HTableDescriptor get(final byte [] tablename)
  throws TableExistsException, FileNotFoundException, IOException {
    return get(Bytes.toString(tablename));
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.TableDescriptors#getTableDescriptor(byte[])
   */
  @Override
  public HTableDescriptor get(final String tablename)
  throws TableExistsException, FileNotFoundException, IOException {
    invocations++;
    if (HTableDescriptor.ROOT_TABLEDESC.getNameAsString().equals(tablename)) {
      cachehits++;
      return HTableDescriptor.ROOT_TABLEDESC;
    }
    if (HTableDescriptor.META_TABLEDESC.getNameAsString().equals(tablename)) {
      cachehits++;
      return HTableDescriptor.META_TABLEDESC;
    }
    // .META. and -ROOT- is already handled. If some one tries to get the descriptor for
    // .logs, .oldlogs or .corrupt throw an exception.
    if (HConstants.HBASE_NON_USER_TABLE_DIRS.contains(tablename)) {
       throw new IOException("No descriptor found for table = " + tablename);
    }

    // Look in cache of descriptors.
    TableDescriptorModtime tdm = this.cache.get(tablename);

    // Check mod time has not changed (this is trip to NN).
    long modtime = getTableInfoModtime(this.fs, this.rootdir, tablename);
    if (tdm != null) {
      if (modtime <= tdm.getModtime()) {
        cachehits++;
        return tdm.getTableDescriptor();
      }
    }
    HTableDescriptor htd = getTableDescriptor(this.fs, this.rootdir, tablename);
    if (htd == null) {
      // More likely is above will throw a FileNotFoundException
      throw new TableExistsException("No descriptor for " + tablename);
    }
    this.cache.put(tablename, new TableDescriptorModtime(modtime, htd));
    return htd;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.TableDescriptors#getTableDescriptors(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.fs.Path)
   */
  @Override
  public Map<String, HTableDescriptor> getAll()
  throws IOException {
    Map<String, HTableDescriptor> htds = new TreeMap<String, HTableDescriptor>();
    List<Path> tableDirs = FSUtils.getTableDirs(fs, rootdir);
    for (Path d: tableDirs) {
      HTableDescriptor htd = null;
      try {

        htd = get(d.getName());
      } catch (FileNotFoundException fnfe) {
        // inability of retrieving one HTD shouldn't stop getting the remaining
        LOG.warn("Trouble retrieving htd", fnfe);
      }
      if (htd == null) continue;
      htds.put(d.getName(), htd);
    }
    return htds;
  }

  @Override
  public void add(HTableDescriptor htd) throws IOException {
    if (Bytes.equals(HConstants.ROOT_TABLE_NAME, htd.getName())) {
      throw new NotImplementedException();
    }
    if (Bytes.equals(HConstants.META_TABLE_NAME, htd.getName())) {
      throw new NotImplementedException();
    }
    if (HConstants.HBASE_NON_USER_TABLE_DIRS.contains(htd.getNameAsString())) {
      throw new NotImplementedException();
    }
    if (!this.fsreadonly) updateHTableDescriptor(this.fs, this.rootdir, htd);
    long modtime = getTableInfoModtime(this.fs, this.rootdir, htd.getNameAsString());
    this.cache.put(htd.getNameAsString(), new TableDescriptorModtime(modtime, htd));
  }

  @Override
  public HTableDescriptor remove(final String tablename)
  throws IOException {
    if (!this.fsreadonly) {
      Path tabledir = FSUtils.getTablePath(this.rootdir, tablename);
      if (this.fs.exists(tabledir)) {
        if (!this.fs.delete(tabledir, true)) {
          throw new IOException("Failed delete of " + tabledir.toString());
        }
      }
    }
    TableDescriptorModtime tdm = this.cache.remove(tablename);
    return tdm == null? null: tdm.getTableDescriptor();
  }

  /**
   * Checks if <code>.tableinfo<code> exists for given table
   * 
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @param tableName name of table
   * @return true if exists
   * @throws IOException
   */
  public static boolean isTableInfoExists(FileSystem fs, Path rootdir,
      String tableName) throws IOException {
    FileStatus status = getTableInfoPath(fs, rootdir, tableName);
    return status == null? false: fs.exists(status.getPath());
  }

  private static FileStatus getTableInfoPath(final FileSystem fs,
      final Path rootdir, final String tableName)
  throws IOException {
    Path tabledir = FSUtils.getTablePath(rootdir, tableName);
    return getTableInfoPath(fs, tabledir);
  }

  /**
   * Looks under the table directory in the filesystem for files with a
   * {@link #TABLEINFO_NAME} prefix.  Returns reference to the 'latest' instance.
   * @param fs
   * @param tabledir
   * @return The 'current' tableinfo file.
   * @throws IOException
   */
  private static FileStatus getTableInfoPath(final FileSystem fs,
      final Path tabledir)
  throws IOException {
    FileStatus [] status = fs.listStatus(tabledir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        // Accept any file that starts with TABLEINFO_NAME
        return p.getName().startsWith(TABLEINFO_NAME);
      }
    });
    if (status == null || status.length < 1) return null;
    Arrays.sort(status, new FileStatusFileNameComparator());
    if (status.length > 1) {
      // Clean away old versions of .tableinfo
      for (int i = 1; i < status.length; i++) {
        Path p = status[i].getPath();
        // Clean up old versions
        if (!fs.delete(p, false)) {
          LOG.warn("Failed cleanup of " + status);
        } else {
          LOG.debug("Cleaned up old tableinfo file " + p);
        }
      }
    }
    return status[0];
  }

  /**
   * Compare {@link FileStatus} instances by {@link Path#getName()}.
   * Returns in reverse order.
   */
  static class FileStatusFileNameComparator
  implements Comparator<FileStatus> {
    @Override
    public int compare(FileStatus left, FileStatus right) {
      return -left.compareTo(right);
    }
  }

  /**
   * Width of the sequenceid that is a suffix on a tableinfo file.
   */
  static final int WIDTH_OF_SEQUENCE_ID = 10;

  /*
   * @param number Number to use as suffix.
   * @return Returns zero-prefixed 5-byte wide decimal version of passed
   * number (Does absolute in case number is negative).
   */
  static String formatTableInfoSequenceId(final int number) {
    byte [] b = new byte[WIDTH_OF_SEQUENCE_ID];
    int d = Math.abs(number);
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte)((d % 10) + '0');
      d /= 10;
    }
    return Bytes.toString(b);
  }

  /**
   * Regex to eat up sequenceid suffix on a .tableinfo file.
   * Use regex because may encounter oldstyle .tableinfos where there is no
   * sequenceid on the end.
   */
  private static final Pattern SUFFIX =
    Pattern.compile(TABLEINFO_NAME + "(\\.([0-9]{" + WIDTH_OF_SEQUENCE_ID + "}))?$");


  /**
   * @param p Path to a <code>.tableinfo</code> file.
   * @return The current editid or 0 if none found.
   */
  static int getTableInfoSequenceid(final Path p) {
    if (p == null) return 0;
    Matcher m = SUFFIX.matcher(p.getName());
    if (!m.matches()) throw new IllegalArgumentException(p.toString());
    String suffix = m.group(2);
    if (suffix == null || suffix.length() <= 0) return 0;
    return Integer.parseInt(m.group(2));
  }

  /**
   * @param tabledir
   * @param sequenceid
   * @return Name of tableinfo file.
   */
  static Path getTableInfoFileName(final Path tabledir, final int sequenceid) {
    return new Path(tabledir,
      TABLEINFO_NAME + "." + formatTableInfoSequenceId(sequenceid));
  }

  /**
   * @param fs
   * @param rootdir
   * @param tableName
   * @return Modification time for the table {@link #TABLEINFO_NAME} file
   * or <code>0</code> if no tableinfo file found.
   * @throws IOException
   */
  static long getTableInfoModtime(final FileSystem fs, final Path rootdir,
      final String tableName)
  throws IOException {
    FileStatus status = getTableInfoPath(fs, rootdir, tableName);
    return status == null? 0: status.getModificationTime();
  }

  /**
   * Get HTD from HDFS.
   * @param fs
   * @param hbaseRootDir
   * @param tableName
   * @return Descriptor or null if none found.
   * @throws IOException
   */
  public static HTableDescriptor getTableDescriptor(FileSystem fs,
      Path hbaseRootDir, byte[] tableName)
  throws IOException {
     return getTableDescriptor(fs, hbaseRootDir, Bytes.toString(tableName));
  }

  static HTableDescriptor getTableDescriptor(FileSystem fs,
      Path hbaseRootDir, String tableName) {
    HTableDescriptor htd = null;
    try {
      htd = getTableDescriptor(fs, FSUtils.getTablePath(hbaseRootDir, tableName));
    } catch (NullPointerException e) {
      LOG.debug("Exception during readTableDecriptor. Current table name = " +
        tableName , e);
    } catch (IOException ioe) {
      LOG.debug("Exception during readTableDecriptor. Current table name = " +
        tableName , ioe);
    }
    return htd;
  }

  public static HTableDescriptor getTableDescriptor(FileSystem fs, Path tableDir)
  throws IOException, NullPointerException {
    if (tableDir == null) throw new NullPointerException();
    FileStatus status = getTableInfoPath(fs, tableDir);
    if (status == null) return null;
    FSDataInputStream fsDataInputStream = fs.open(status.getPath());
    HTableDescriptor hTableDescriptor = null;
    try {
      hTableDescriptor = new HTableDescriptor();
      hTableDescriptor.readFields(fsDataInputStream);
    } finally {
      fsDataInputStream.close();
    }
    return hTableDescriptor;
  }

  /**
   * Update table descriptor
   * @param fs
   * @param conf
   * @param hTableDescriptor
   * @return New tableinfo or null if we failed update.
   * @throws IOException Thrown if failed update.
   */
  static Path updateHTableDescriptor(FileSystem fs, Path rootdir,
      HTableDescriptor hTableDescriptor)
  throws IOException {
    Path tableDir = FSUtils.getTablePath(rootdir, hTableDescriptor.getName());
    Path p = writeTableDescriptor(fs, hTableDescriptor, tableDir,
      getTableInfoPath(fs, tableDir));
    if (p == null) throw new IOException("Failed update");
    LOG.info("Updated tableinfo=" + p);
    return p;
  }

  /**
   * Deletes a table's directory from the file system if exists. Used in unit
   * tests.
   */
  public static void deleteTableDescriptorIfExists(String tableName,
      Configuration conf) throws IOException {
    FileSystem fs = FSUtils.getCurrentFileSystem(conf);
    FileStatus status = getTableInfoPath(fs, FSUtils.getRootDir(conf), tableName);
    // The below deleteDirectory works for either file or directory.
    if (fs.exists(status.getPath())) FSUtils.deleteDirectory(fs, status.getPath());
  }

  /**
   * @param fs
   * @param hTableDescriptor
   * @param tableDir
   * @param status
   * @return Descriptor file or null if we failed write.
   * @throws IOException 
   */
  private static Path writeTableDescriptor(final FileSystem fs,
      final HTableDescriptor hTableDescriptor, final Path tableDir,
      final FileStatus status)
  throws IOException {
    // Get temporary dir into which we'll first write a file to avoid
    // half-written file phenomeon.
    Path tmpTableDir = new Path(tableDir, ".tmp");
    // What is current sequenceid?  We read the current sequenceid from
    // the current file.  After we read it, another thread could come in and
    // compete with us writing out next version of file.  The below retries
    // should help in this case some but its hard to do guarantees in face of
    // concurrent schema edits.
    int currentSequenceid =
      status == null? 0: getTableInfoSequenceid(status.getPath());
    int sequenceid = currentSequenceid;
    // Put arbitrary upperbound on how often we retry
    int retries = 10;
    int retrymax = currentSequenceid + retries;
    Path tableInfoPath = null;
    do {
      sequenceid += 1;
      Path p = getTableInfoFileName(tmpTableDir, sequenceid);
      if (fs.exists(p)) {
        LOG.debug(p + " exists; retrying up to " + retries + " times");
        continue;
      }
      try {
        writeHTD(fs, p, hTableDescriptor);
        tableInfoPath = getTableInfoFileName(tableDir, sequenceid);
        if (!fs.rename(p, tableInfoPath)) {
          throw new IOException("Failed rename of " + p + " to " + tableInfoPath);
        }
      } catch (IOException ioe) {
        // Presume clash of names or something; go around again.
        LOG.debug("Failed write and/or rename; retrying", ioe);
        if (!FSUtils.deleteDirectory(fs, p)) {
          LOG.warn("Failed cleanup of " + p);
        }
        tableInfoPath = null;
        continue;
      }
      // Cleanup old schema file.
      if (status != null) {
        if (!FSUtils.deleteDirectory(fs, status.getPath())) {
          LOG.warn("Failed delete of " + status.getPath() + "; continuing");
        }
      }
      break;
    } while (sequenceid < retrymax);
    return tableInfoPath;
  }

  private static void writeHTD(final FileSystem fs, final Path p,
      final HTableDescriptor htd)
  throws IOException {
    FSDataOutputStream out = fs.create(p, false);
    try {
      htd.write(out);
      out.write('\n');
      out.write('\n');
      out.write(Bytes.toBytes(htd.toString()));
    } finally {
      out.close();
    }
  }

  /**
   * Create new HTableDescriptor in HDFS. Happens when we are creating table.
   * 
   * @param htableDescriptor
   * @param conf
   */
  public static boolean createTableDescriptor(final HTableDescriptor htableDescriptor,
      Configuration conf)
  throws IOException {
    return createTableDescriptor(htableDescriptor, conf, false);
  }

  /**
   * Create new HTableDescriptor in HDFS. Happens when we are creating table. If
   * forceCreation is true then even if previous table descriptor is present it
   * will be overwritten
   * 
   * @param htableDescriptor
   * @param conf
   * @param forceCreation True if we are to overwrite existing file.
   */
  static boolean createTableDescriptor(final HTableDescriptor htableDescriptor,
      final Configuration conf, boolean forceCreation)
  throws IOException {
    FileSystem fs = FSUtils.getCurrentFileSystem(conf);
    return createTableDescriptor(fs, FSUtils.getRootDir(conf), htableDescriptor,
        forceCreation);
  }

  /**
   * Create new HTableDescriptor in HDFS. Happens when we are creating table.
   * Used by tests.
   * @param fs
   * @param htableDescriptor
   * @param rootdir
   */
  public static boolean createTableDescriptor(FileSystem fs, Path rootdir,
      HTableDescriptor htableDescriptor)
  throws IOException {
    return createTableDescriptor(fs, rootdir, htableDescriptor, false);
  }

  /**
   * Create new HTableDescriptor in HDFS. Happens when we are creating table. If
   * forceCreation is true then even if previous table descriptor is present it
   * will be overwritten
   * 
   * @param fs
   * @param htableDescriptor
   * @param rootdir
   * @param forceCreation
   * @return True if we successfully created file.
   */
  public static boolean createTableDescriptor(FileSystem fs, Path rootdir,
      HTableDescriptor htableDescriptor, boolean forceCreation)
  throws IOException {
    FileStatus status =
      getTableInfoPath(fs, rootdir, htableDescriptor.getNameAsString());
    if (status != null) {
      LOG.info("Current tableInfoPath = " + status.getPath());
      if (!forceCreation) {
        if (fs.exists(status.getPath()) && status.getLen() > 0) {
          LOG.info("TableInfo already exists.. Skipping creation");
          return false;
        }
      }
    }
    Path p = writeTableDescriptor(fs, htableDescriptor,
      FSUtils.getTablePath(rootdir, htableDescriptor.getNameAsString()), status);
    return p != null;
  }
}