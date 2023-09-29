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
package org.apache.hadoop.fs.viewfs;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

/**
 * Nfly is a multi filesystem mount point.
 */
@Private
final class NflyFSystem extends FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(NflyFSystem.class);
  private static final String NFLY_TMP_PREFIX = "_nfly_tmp_";

  enum NflyKey {
    // minimum replication, if local filesystem is included +1 is recommended
    minReplication,

    // forces to check all the replicas and fetch the one with the most recent
    // time stamp
    //
    readMostRecent,

    // create missing replica from far to near, including local?
    repairOnRead
  }

  private static final int DEFAULT_MIN_REPLICATION = 2;
  private static URI nflyURI = URI.create("nfly:///");

  private final NflyNode[] nodes;
  private final int minReplication;
  private final EnumSet<NflyKey> nflyFlags;
  private final Node myNode;
  private final NetworkTopology topology;

  /**
   * URI's authority is used as an approximation of the distance from the
   * client. It's sufficient for DC but not accurate because worker nodes can be
   * closer.
   */
  private static class NflyNode extends NodeBase {
    private final ChRootedFileSystem fs;
    NflyNode(String hostName, String rackName, URI uri,
        Configuration conf) throws IOException {
      this(hostName, rackName, new ChRootedFileSystem(uri, conf));
    }

    NflyNode(String hostName, String rackName, ChRootedFileSystem fs) {
      super(hostName, rackName);
      this.fs = fs;
    }

    ChRootedFileSystem getFs() {
      return fs;
    }

    @Override
    public boolean equals(Object o) {
      // satisfy findbugs
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      // satisfy findbugs
      return super.hashCode();
    }

  }

  private static final class MRNflyNode
      extends NflyNode implements Comparable<MRNflyNode> {

    private FileStatus status;

    private MRNflyNode(NflyNode n) {
      super(n.getName(), n.getNetworkLocation(), n.fs);
    }

    private void updateFileStatus(Path f) throws IOException {
      final FileStatus tmpStatus = getFs().getFileStatus(f);
      status = tmpStatus == null
          ? notFoundStatus(f)
          : tmpStatus;
    }

    // TODO allow configurable error margin for FileSystems with different
    // timestamp precisions
    @Override
    public int compareTo(MRNflyNode other) {
      if (status == null) {
        return other.status == null ? 0 : 1; // move non-null towards head
      } else if (other.status == null) {
        return -1; // move this towards head
      } else {
        final long mtime = status.getModificationTime();
        final long their = other.status.getModificationTime();
        return Long.compare(their, mtime); // move more recent towards head
      }
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MRNflyNode)) {
        return false;
      }
      MRNflyNode other = (MRNflyNode) o;
      return 0 == compareTo(other);
    }

    @Override
    public int hashCode() {
      // satisfy findbugs
      return super.hashCode();
    }

    private FileStatus nflyStatus() throws IOException {
      return new NflyStatus(getFs(), status);
    }

    private FileStatus cloneStatus() throws IOException {
      return new FileStatus(status.getLen(),
          status.isDirectory(),
          status.getReplication(),
          status.getBlockSize(),
          status.getModificationTime(),
          status.getAccessTime(),
          null, null, null,
          status.isSymlink() ? status.getSymlink() : null,
          status.getPath());
    }
  }

  private MRNflyNode[] workSet() {
    final MRNflyNode[] res = new MRNflyNode[nodes.length];
    for (int i = 0; i < res.length; i++) {
      res[i] = new MRNflyNode(nodes[i]);
    }
    return res;
  }


  /**
   * Utility to replace null with DEFAULT_RACK.
   *
   * @param rackString rack value, can be null
   * @return non-null rack string
   */
  private static String getRack(String rackString) {
    return rackString == null ? NetworkTopology.DEFAULT_RACK : rackString;
  }

  /**
   * Creates a new Nfly instance.
   *
   * @param uris the list of uris in the mount point
   * @param conf configuration object
   * @param minReplication minimum copies to commit a write op
   * @param nflyFlags modes such readMostRecent
   * @throws IOException
   */
  private NflyFSystem(URI[] uris, Configuration conf, int minReplication,
      EnumSet<NflyKey> nflyFlags) throws IOException {
    this(uris, conf, minReplication, nflyFlags, null);
  }

  /**
   * Creates a new Nfly instance.
   *
   * @param uris the list of uris in the mount point
   * @param conf configuration object
   * @param minReplication minimum copies to commit a write op
   * @param nflyFlags modes such readMostRecent
   * @param fsGetter to get the file system instance with the given uri
   * @throws IOException
   */
  private NflyFSystem(URI[] uris, Configuration conf, int minReplication,
      EnumSet<NflyKey> nflyFlags, FsGetter fsGetter) throws IOException {
    if (uris.length < minReplication) {
      throw new IOException(minReplication + " < " + uris.length
          + ": Minimum replication < #destinations");
    }
    setConf(conf);
    final String localHostName = InetAddress.getLocalHost().getHostName();

    // build a list for topology resolution
    final List<String> hostStrings = new ArrayList<String>(uris.length + 1);
    for (URI uri : uris) {
      final String uriHost = uri.getHost();
      // assume local file system or another closest filesystem if no authority
      hostStrings.add(uriHost == null ? localHostName : uriHost);
    }
    // resolve the client node
    hostStrings.add(localHostName);

    final DNSToSwitchMapping tmpDns = ReflectionUtils.newInstance(conf.getClass(
        CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        ScriptBasedMapping.class, DNSToSwitchMapping.class), conf);

    // this is an ArrayList
    final List<String> rackStrings = tmpDns.resolve(hostStrings);
    nodes = new NflyNode[uris.length];
    final Iterator<String> rackIter = rackStrings.iterator();
    for (int i = 0; i < nodes.length; i++) {
      if (fsGetter != null) {
        nodes[i] = new NflyNode(hostStrings.get(i), rackIter.next(),
            new ChRootedFileSystem(fsGetter.getNewInstance(uris[i], conf),
                uris[i]));
      } else {
        nodes[i] =
            new NflyNode(hostStrings.get(i), rackIter.next(), uris[i], conf);
      }
    }
    // sort all the uri's by distance from myNode, the local file system will
    // automatically be the the first one.
    //
    myNode = new NodeBase(localHostName, getRack(rackIter.next()));
    topology = NetworkTopology.getInstance(conf);
    topology.sortByDistance(myNode, nodes, nodes.length);

    this.minReplication = minReplication;
    this.nflyFlags = nflyFlags;
    statistics = getStatistics(nflyURI.getScheme(), getClass());
  }

  /**
   * Transactional output stream. When creating path /dir/file
   * 1) create invisible /real/dir_i/_nfly_tmp_file
   * 2) when more than min replication was written, write is committed by
   *   renaming all successfully written files to /real/dir_i/file
   */
  private final class NflyOutputStream extends OutputStream {
    // actual path
    private final Path nflyPath;
    // tmp path before commit
    private final Path tmpPath;
    // broadcast set
    private final FSDataOutputStream[] outputStreams;
    // status set: 1 working, 0 problem
    private final BitSet opSet;
    private final boolean useOverwrite;

    private NflyOutputStream(Path f, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      nflyPath = f;
      tmpPath = getNflyTmpPath(f);
      outputStreams = new FSDataOutputStream[nodes.length];
      for (int i = 0; i < outputStreams.length; i++) {
        outputStreams[i] = nodes[i].fs.create(tmpPath, permission, true,
            bufferSize, replication, blockSize, progress);
      }
      opSet = new BitSet(outputStreams.length);
      opSet.set(0, outputStreams.length);
      useOverwrite = false;
    }

    //
    // TODO consider how to clean up and throw an exception early when the clear
    // bits under min replication
    //

    private void mayThrow(List<IOException> ioExceptions) throws IOException {
      final IOException ioe = MultipleIOException
          .createIOException(ioExceptions);
      if (opSet.cardinality() < minReplication) {
        throw ioe;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Exceptions occurred: " + ioe);
        }
      }
    }


    @Override
    public void write(int d) throws IOException {
      final List<IOException> ioExceptions = new ArrayList<IOException>();
      for (int i = opSet.nextSetBit(0);
           i >=0;
           i = opSet.nextSetBit(i + 1)) {
        try {
          outputStreams[i].write(d);
        } catch (Throwable t) {
          osException(i, "write", t, ioExceptions);
        }
      }
      mayThrow(ioExceptions);
    }

    private void osException(int i, String op, Throwable t,
        List<IOException> ioExceptions) {
      opSet.clear(i);
      processThrowable(nodes[i], op, t, ioExceptions, tmpPath, nflyPath);
    }

    @Override
    public void write(byte[] bytes, int offset, int len) throws IOException {
      final List<IOException> ioExceptions = new ArrayList<IOException>();
      for (int i = opSet.nextSetBit(0);
           i >= 0;
           i = opSet.nextSetBit(i + 1)) {
        try {
          outputStreams[i].write(bytes, offset, len);
        } catch (Throwable t) {
          osException(i, "write", t, ioExceptions);
        }
      }
      mayThrow(ioExceptions);
    }

    @Override
    public void flush() throws IOException {
      final List<IOException> ioExceptions = new ArrayList<IOException>();
      for (int i = opSet.nextSetBit(0);
           i >= 0;
           i = opSet.nextSetBit(i + 1)) {
        try {
          outputStreams[i].flush();
        } catch (Throwable t) {
          osException(i, "flush", t, ioExceptions);
        }
      }
      mayThrow(ioExceptions);
    }

    @Override
    public void close() throws IOException {
      final List<IOException> ioExceptions = new ArrayList<IOException>();
      for (int i = opSet.nextSetBit(0);
           i >= 0;
           i = opSet.nextSetBit(i + 1)) {
        try {
          outputStreams[i].close();
        } catch (Throwable t) {
          osException(i, "close", t, ioExceptions);
        }
      }
      if (opSet.cardinality() < minReplication) {
        cleanupAllTmpFiles();
        throw new IOException("Failed to sufficiently replicate: min="
            + minReplication + " actual=" + opSet.cardinality());
      } else {
        commit();
      }
    }

    private void cleanupAllTmpFiles() throws IOException {
      for (int i = 0; i < outputStreams.length; i++) {
        try {
          nodes[i].fs.delete(tmpPath);
        } catch (Throwable t) {
          processThrowable(nodes[i], "delete", t, null, tmpPath);
        }
      }
    }

    private void commit() throws IOException {
      final List<IOException> ioExceptions = new ArrayList<IOException>();
      for (int i = opSet.nextSetBit(0);
           i >= 0;
           i = opSet.nextSetBit(i + 1)) {
        final NflyNode nflyNode = nodes[i];
        try {
          if (useOverwrite) {
            nflyNode.fs.delete(nflyPath);
          }
          nflyNode.fs.rename(tmpPath, nflyPath);

        } catch (Throwable t) {
          osException(i, "commit", t, ioExceptions);
        }
      }

      if (opSet.cardinality() < minReplication) {
        // cleanup should be done outside. If rename failed, it's unlikely that
        // delete will work either. It's the same kind of metadata-only op
        //
        throw MultipleIOException.createIOException(ioExceptions);
      }

      // best effort to have a consistent timestamp
      final long commitTime = System.currentTimeMillis();
      for (int i = opSet.nextSetBit(0);
          i >= 0;
          i = opSet.nextSetBit(i + 1)) {
        try {
          nodes[i].fs.setTimes(nflyPath, commitTime, commitTime);
        } catch (Throwable t) {
          LOG.info("Failed to set timestamp: " + nodes[i] + " " + nflyPath);
        }
      }
    }
  }

  private Path getNflyTmpPath(Path f) {
    return new Path(f.getParent(), NFLY_TMP_PREFIX + f.getName());
  }

  /**
   * // TODO
   * Some file status implementations have expensive deserialization or metadata
   * retrieval. This probably does not go beyond RawLocalFileSystem. Wrapping
   * the the real file status to preserve this behavior. Otherwise, calling
   * realStatus getters in constructor defeats this design.
   */
  static final class NflyStatus extends FileStatus {
    private static final long serialVersionUID = 0x21f276d8;

    private final FileStatus realStatus;
    private final String strippedRoot;

    private NflyStatus(ChRootedFileSystem realFs, FileStatus realStatus)
        throws IOException {
      this.realStatus = realStatus;
      this.strippedRoot = realFs.stripOutRoot(realStatus.getPath());
    }

    String stripRoot() throws IOException {
      return strippedRoot;
    }

    @Override
    public long getLen() {
      return realStatus.getLen();
    }

    @Override
    public boolean isFile() {
      return realStatus.isFile();
    }

    @Override
    public boolean isDirectory() {
      return realStatus.isDirectory();
    }

    @Override
    public boolean isSymlink() {
      return realStatus.isSymlink();
    }

    @Override
    public long getBlockSize() {
      return realStatus.getBlockSize();
    }

    @Override
    public short getReplication() {
      return realStatus.getReplication();
    }

    @Override
    public long getModificationTime() {
      return realStatus.getModificationTime();
    }

    @Override
    public long getAccessTime() {
      return realStatus.getAccessTime();
    }

    @Override
    public FsPermission getPermission() {
      return realStatus.getPermission();
    }

    @Override
    public String getOwner() {
      return realStatus.getOwner();
    }

    @Override
    public String getGroup() {
      return realStatus.getGroup();
    }

    @Override
    public Path getPath() {
      return realStatus.getPath();
    }

    @Override
    public void setPath(Path p) {
      realStatus.setPath(p);
    }

    @Override
    public Path getSymlink() throws IOException {
      return realStatus.getSymlink();
    }

    @Override
    public void setSymlink(Path p) {
      realStatus.setSymlink(p);
    }

    @Override
    public boolean equals(Object o) {
      return realStatus.equals(o);
    }

    @Override
    public int hashCode() {
      return realStatus.hashCode();
    }

    @Override
    public String toString() {
      return realStatus.toString();
    }
  }

  @Override
  public URI getUri() {
    return nflyURI;
  }

  /**
   * Category: READ.
   *
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   * @return input stream according to nfly flags (closest, most recent)
   * @throws IOException
   * @throws FileNotFoundException iff all destinations generate this exception
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // TODO proxy stream for reads
    final List<IOException> ioExceptions =
        new ArrayList<IOException>(nodes.length);
    int numNotFounds = 0;
    final MRNflyNode[] mrNodes = workSet();

    // naively iterate until one can be opened
    //
    for (final MRNflyNode nflyNode : mrNodes) {
      try {
        if (nflyFlags.contains(NflyKey.repairOnRead)
            || nflyFlags.contains(NflyKey.readMostRecent)) {
          // calling file status to avoid pulling bytes prematurely
          nflyNode.updateFileStatus(f);
        } else {
          return nflyNode.getFs().open(f, bufferSize);
        }
      } catch (FileNotFoundException fnfe) {
        nflyNode.status = notFoundStatus(f);
        numNotFounds++;
        processThrowable(nflyNode, "open", fnfe, ioExceptions, f);
      } catch (Throwable t) {
        processThrowable(nflyNode, "open", t, ioExceptions, f);
      }
    }

    if (nflyFlags.contains(NflyKey.readMostRecent)) {
      // sort from most recent to least recent
      Arrays.sort(mrNodes);
    }

    final FSDataInputStream fsdisAfterRepair = repairAndOpen(mrNodes, f,
        bufferSize);

    if (fsdisAfterRepair != null) {
      return fsdisAfterRepair;
    }

    mayThrowFileNotFound(ioExceptions, numNotFounds);
    throw MultipleIOException.createIOException(ioExceptions);
  }

  private static FileStatus notFoundStatus(Path f) {
    return new FileStatus(-1, false, 0, 0, 0, f);
  }

  /**
   * Iterate all available nodes in the proximity order to attempt repair of all
   * FileNotFound nodes.
   *
   * @param mrNodes work set copy of nodes
   * @param f path to repair and open
   * @param bufferSize buffer size for read RPC
   * @return the closest/most recent replica stream AFTER repair
   */
  private FSDataInputStream repairAndOpen(MRNflyNode[] mrNodes, Path f,
      int bufferSize) {
    long maxMtime = 0L;
    for (final MRNflyNode srcNode : mrNodes) {
      if (srcNode.status == null  // not available
          || srcNode.status.getLen() < 0L) { // not found
        continue; // not available
      }
      if (srcNode.status.getModificationTime() > maxMtime) {
        maxMtime = srcNode.status.getModificationTime();
      }

      // attempt to repair all notFound nodes with srcNode
      //
      for (final MRNflyNode dstNode : mrNodes) {
        if (dstNode.status == null // not available
            || srcNode.compareTo(dstNode) == 0) { // same mtime
          continue;
        }

        try {
          // status is absolute from the underlying mount, making it chrooted
          //
          final FileStatus srcStatus = srcNode.cloneStatus();
          srcStatus.setPath(f);
          final Path tmpPath = getNflyTmpPath(f);
          FileUtil.copy(srcNode.getFs(), srcStatus, dstNode.getFs(), tmpPath,
              false,                // don't delete
              true,                 // overwrite
              getConf());
          dstNode.getFs().delete(f, false);
          if (dstNode.getFs().rename(tmpPath, f)) {
            try {
              dstNode.getFs().setTimes(f, srcNode.status.getModificationTime(),
                  srcNode.status.getAccessTime());
            } finally {
              // save getFileStatus rpc
              srcStatus.setPath(dstNode.getFs().makeQualified(f));
              dstNode.status = srcStatus;
            }
          }
        } catch (IOException ioe) {
          // can blame the source by statusSet.clear(ai), however, it would
          // cost an extra RPC, so just rely on the loop below that will attempt
          // an open anyhow
          //
          LOG.info(f + " " + srcNode + "->" + dstNode + ": Failed to repair",
                ioe);
        }
      }
    }

    // Since Java7, QuickSort is used instead of MergeSort.
    // QuickSort may not be stable and thus the equal most recent nodes, may no
    // longer appear in the NetworkTopology order.
    //
    if (maxMtime > 0) {
      final List<MRNflyNode> mrList = new ArrayList<MRNflyNode>();
      for (final MRNflyNode openNode : mrNodes) {
        if (openNode.status != null && openNode.status.getLen() >= 0L) {
          if (openNode.status.getModificationTime() == maxMtime) {
            mrList.add(openNode);
          }
        }
      }
      // assert mrList.size > 0
      final MRNflyNode[] readNodes = mrList.toArray(new MRNflyNode[0]);
      topology.sortByDistance(myNode, readNodes, readNodes.length);
      for (final MRNflyNode rNode : readNodes) {
        try {
          return rNode.getFs().open(f, bufferSize);
        } catch (IOException e) {
          LOG.info(f + ": Failed to open at " + rNode.getFs().getUri());
        }
      }
    }
    return null;
  }

  private void mayThrowFileNotFound(List<IOException> ioExceptions,
      int numNotFounds) throws FileNotFoundException {
    if (numNotFounds == nodes.length) {
      throw (FileNotFoundException)ioExceptions.get(nodes.length - 1);
    }
  }

  // WRITE
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return new FSDataOutputStream(new NflyOutputStream(f, permission, overwrite,
        bufferSize, replication, blockSize, progress), statistics);
  }

  // WRITE
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return null;
  }

  // WRITE
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    final List<IOException> ioExceptions = new ArrayList<IOException>();
    int numNotFounds = 0;
    boolean succ = true;
    for (final NflyNode nflyNode : nodes) {
      try {
        succ &= nflyNode.fs.rename(src, dst);
      } catch (FileNotFoundException fnfe) {
        numNotFounds++;
        processThrowable(nflyNode, "rename", fnfe, ioExceptions, src, dst);
      } catch (Throwable t) {
        processThrowable(nflyNode, "rename", t, ioExceptions, src, dst);
        succ = false;
      }
    }

    mayThrowFileNotFound(ioExceptions, numNotFounds);

    // if all destinations threw exceptions throw, otherwise return
    //
    if (ioExceptions.size() == nodes.length) {
      throw MultipleIOException.createIOException(ioExceptions);
    }

    return succ;
  }

  // WRITE
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    final List<IOException> ioExceptions = new ArrayList<IOException>();
    int numNotFounds = 0;
    boolean succ = true;
    for (final NflyNode nflyNode : nodes) {
      try {
        succ &= nflyNode.fs.delete(f);
      } catch (FileNotFoundException fnfe) {
        numNotFounds++;
        processThrowable(nflyNode, "delete", fnfe, ioExceptions, f);
      } catch (Throwable t) {
        processThrowable(nflyNode, "delete", t, ioExceptions, f);
        succ = false;
      }
    }
    mayThrowFileNotFound(ioExceptions, numNotFounds);

    // if all destinations threw exceptions throw, otherwise return
    //
    if (ioExceptions.size() == nodes.length) {
      throw MultipleIOException.createIOException(ioExceptions);
    }

    return succ;
  }


  /**
   * Returns the closest non-failing destination's result.
   *
   * @param f given path
   * @return array of file statuses according to nfly modes
   * @throws FileNotFoundException
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    final List<IOException> ioExceptions =
        new ArrayList<IOException>(nodes.length);

    final MRNflyNode[] mrNodes = workSet();
    if (nflyFlags.contains(NflyKey.readMostRecent)) {
      int numNotFounds = 0;
      for (final MRNflyNode nflyNode : mrNodes) {
        try {
          nflyNode.updateFileStatus(f);
        } catch (FileNotFoundException fnfe) {
          numNotFounds++;
          processThrowable(nflyNode, "listStatus", fnfe, ioExceptions, f);
        } catch (Throwable t) {
          processThrowable(nflyNode, "listStatus", t, ioExceptions, f);
        }
      }
      mayThrowFileNotFound(ioExceptions, numNotFounds);
      Arrays.sort(mrNodes);
    }

    int numNotFounds = 0;
    for (final MRNflyNode nflyNode : mrNodes) {
      try {
        final FileStatus[] realStats = nflyNode.getFs().listStatus(f);
        final FileStatus[] nflyStats = new FileStatus[realStats.length];
        for (int i = 0; i < realStats.length; i++) {
          nflyStats[i] = new NflyStatus(nflyNode.getFs(), realStats[i]);
        }
        return nflyStats;
      } catch (FileNotFoundException fnfe) {
        numNotFounds++;
        processThrowable(nflyNode, "listStatus", fnfe, ioExceptions, f);
      } catch (Throwable t) {
        processThrowable(nflyNode, "listStatus", t, ioExceptions, f);
      }
    }
    mayThrowFileNotFound(ioExceptions, numNotFounds);
    throw MultipleIOException.createIOException(ioExceptions);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws FileNotFoundException, IOException {
    // TODO important for splits
    return super.listLocatedStatus(f);
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    for (final NflyNode nflyNode : nodes) {
      nflyNode.fs.setWorkingDirectory(newDir);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return nodes[0].fs.getWorkingDirectory(); // 0 is as good as any
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    boolean succ = true;
    for (final NflyNode nflyNode : nodes) {
      succ &= nflyNode.fs.mkdirs(f, permission);
    }
    return succ;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    // TODO proxy stream for reads
    final List<IOException> ioExceptions =
        new ArrayList<IOException>(nodes.length);
    int numNotFounds = 0;
    final MRNflyNode[] mrNodes = workSet();

    long maxMtime = Long.MIN_VALUE;
    int maxMtimeIdx = Integer.MIN_VALUE;

    // naively iterate until one can be returned
    //
    for (int i = 0; i < mrNodes.length; i++) {
      MRNflyNode nflyNode = mrNodes[i];
      try {
        nflyNode.updateFileStatus(f);
        if (nflyFlags.contains(NflyKey.readMostRecent)) {
          final long nflyTime = nflyNode.status.getModificationTime();
          if (nflyTime > maxMtime) {
            maxMtime = nflyTime;
            maxMtimeIdx = i;
          }
        } else {
          return nflyNode.nflyStatus();
        }
      } catch (FileNotFoundException fnfe) {
        numNotFounds++;
        processThrowable(nflyNode, "getFileStatus", fnfe, ioExceptions, f);
      } catch (Throwable t) {
        processThrowable(nflyNode, "getFileStatus", t, ioExceptions, f);
      }
    }

    if (maxMtimeIdx >= 0) {
      return mrNodes[maxMtimeIdx].nflyStatus();
    }

    mayThrowFileNotFound(ioExceptions, numNotFounds);
    throw MultipleIOException.createIOException(ioExceptions);
  }

  private static void processThrowable(NflyNode nflyNode, String op,
      Throwable t, List<IOException> ioExceptions,
      Path... f) {
    final String errMsg = Arrays.toString(f)
        + ": failed to " + op + " " + nflyNode.fs.getUri();
    final IOException ioex;
    if (t instanceof FileNotFoundException) {
      ioex = new FileNotFoundException(errMsg);
      ioex.initCause(t);
    } else {
      ioex = new IOException(errMsg, t);
    }

    if (ioExceptions != null) {
      ioExceptions.add(ioex);
    }
  }

  /**
   * Initializes an nfly mountpoint in viewfs.
   *
   * @param uris destinations to replicate writes to
   * @param conf file system configuration
   * @param settings comma-separated list of k=v pairs.
   * @return an Nfly filesystem
   * @throws IOException
   */
  static FileSystem createFileSystem(URI[] uris, Configuration conf,
      String settings, FsGetter fsGetter) throws IOException {
    // assert settings != null
    int minRepl = DEFAULT_MIN_REPLICATION;
    EnumSet<NflyKey> nflyFlags = EnumSet.noneOf(NflyKey.class);
    final String[] kvPairs = StringUtils.split(settings);
    for (String kv : kvPairs) {
      final String[] kvPair = StringUtils.split(kv, '=');
      if (kvPair.length != 2) {
        throw new IllegalArgumentException(kv);
      }
      NflyKey nflyKey = NflyKey.valueOf(kvPair[0]);
      switch (nflyKey) {
      case minReplication:
        minRepl = Integer.parseInt(kvPair[1]);
        break;
      case repairOnRead:
      case readMostRecent:
        if (Boolean.valueOf(kvPair[1])) {
          nflyFlags.add(nflyKey);
        }
        break;
      default:
        throw new IllegalArgumentException(nflyKey + ": Infeasible");
      }
    }
    return new NflyFSystem(uris, conf, minRepl, nflyFlags, fsGetter);
  }
}
