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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Implementation of {@link TableDescriptors} that reads descriptors from the
 * passed filesystem.  It expects descriptors to be in a file under the
 * table's directory in FS.  Can be read-only -- i.e. does not modify
 * the filesystem or can be read and write.
 */
public class FSTableDescriptors implements TableDescriptors {

  private static final Log LOG = LogFactory.getLog(FSTableDescriptors.class);
  private final FileSystem fs;
  private final Path rootdir;
  private final boolean fsreadonly;
  long cachehits = 0;
  long invocations = 0;

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
    long modtime =
      FSUtils.getTableInfoModtime(this.fs, this.rootdir, tablename);
    if (tdm != null) {
      if (modtime <= tdm.getModtime()) {
        cachehits++;
        return tdm.getTableDescriptor();
      }
    }
    HTableDescriptor htd =
      FSUtils.getTableDescriptor(this.fs, this.rootdir, tablename);
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
    if (!this.fsreadonly) FSUtils.updateHTableDescriptor(this.fs, this.rootdir, htd);
    long modtime =
      FSUtils.getTableInfoModtime(this.fs, this.rootdir, htd.getNameAsString());
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
}
