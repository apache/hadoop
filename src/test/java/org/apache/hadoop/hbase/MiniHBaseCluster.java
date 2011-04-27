/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.MapWritable;
import org.apache.zookeeper.KeeperException;

/**
 * This class creates a single process HBase cluster.
 * each server.  The master uses the 'default' FileSystem.  The RegionServers,
 * if we are running on DistributedFilesystem, create a FileSystem instance
 * each and will close down their instance on the way out.
 */
public class MiniHBaseCluster {
  static final Log LOG = LogFactory.getLog(MiniHBaseCluster.class.getName());
  private Configuration conf;
  public LocalHBaseCluster hbaseCluster;
  private static int index;

  /**
   * Start a MiniHBaseCluster.
   * @param conf Configuration to be used for cluster
   * @param numRegionServers initial number of region servers to start.
   * @throws IOException
   */
  public MiniHBaseCluster(Configuration conf, int numRegionServers)
  throws IOException, InterruptedException {
    this(conf, 1, numRegionServers);
  }

  /**
   * Start a MiniHBaseCluster.
   * @param conf Configuration to be used for cluster
   * @param numMasters initial number of masters to start.
   * @param numRegionServers initial number of region servers to start.
   * @throws IOException
   */
  public MiniHBaseCluster(Configuration conf, int numMasters,
      int numRegionServers)
  throws IOException, InterruptedException {
    this.conf = conf;
    conf.set(HConstants.MASTER_PORT, "0");
    init(numMasters, numRegionServers);
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Override Master so can add inject behaviors testing.
   */
  public static class MiniHBaseClusterMaster extends HMaster {
    private final Map<HServerInfo, List<HMsg>> messages =
      new ConcurrentHashMap<HServerInfo, List<HMsg>>();

    private final Map<HServerInfo, IOException> exceptions =
      new ConcurrentHashMap<HServerInfo, IOException>();

    public MiniHBaseClusterMaster(final Configuration conf)
    throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    /**
     * Add a message to send to a regionserver next time it checks in.
     * @param hsi RegionServer's HServerInfo.
     * @param msg Message to add.
     */
    void addMessage(final HServerInfo hsi, HMsg msg) {
      synchronized(this.messages) {
        List<HMsg> hmsgs = this.messages.get(hsi);
        if (hmsgs == null) {
          hmsgs = new ArrayList<HMsg>();
          this.messages.put(hsi, hmsgs);
        }
        hmsgs.add(msg);
      }
    }

    void addException(final HServerInfo hsi, final IOException ex) {
      this.exceptions.put(hsi, ex);
    }

    /**
     * This implementation is special, exceptions will be treated first and
     * message won't be sent back to the region servers even if some are
     * specified.
     * @param hsi the rs
     * @param msgs Messages to add to
     * @return
     * @throws IOException will be throw if any added for this region server
     */
    @Override
    protected HMsg[] adornRegionServerAnswer(final HServerInfo hsi,
        final HMsg[] msgs) throws IOException {
      IOException ex = this.exceptions.remove(hsi);
      if (ex != null) {
        throw ex;
      }
      HMsg [] answerMsgs = msgs;
      synchronized (this.messages) {
        List<HMsg> hmsgs = this.messages.get(hsi);
        if (hmsgs != null && !hmsgs.isEmpty()) {
          int size = answerMsgs.length;
          HMsg [] newAnswerMsgs = new HMsg[size + hmsgs.size()];
          System.arraycopy(answerMsgs, 0, newAnswerMsgs, 0, answerMsgs.length);
          for (int i = 0; i < hmsgs.size(); i++) {
            newAnswerMsgs[answerMsgs.length + i] = hmsgs.get(i);
          }
          answerMsgs = newAnswerMsgs;
          hmsgs.clear();
        }
      }
      return super.adornRegionServerAnswer(hsi, answerMsgs);
    }
  }

  /**
   * Subclass so can get at protected methods (none at moment).  Also, creates
   * a FileSystem instance per instantiation.  Adds a shutdown own FileSystem
   * on the way out. Shuts down own Filesystem only, not All filesystems as
   * the FileSystem system exit hook does.
   */
  public static class MiniHBaseClusterRegionServer extends HRegionServer {
    private Thread shutdownThread = null;
    private User user = null;
    public static boolean TEST_SKIP_CLOSE = false;

    public MiniHBaseClusterRegionServer(Configuration conf)
        throws IOException, InterruptedException {
      super(conf);
      this.user = User.getCurrent();
    }

    @Override
    public boolean closeRegion(HRegionInfo region)
        throws IOException {
      if (TEST_SKIP_CLOSE) return true;
      return super.closeRegion(region);
    }

    public void setHServerInfo(final HServerInfo hsi) {
      this.serverInfo = hsi;
    }

    /*
     * @param c
     * @param currentfs We return this if we did not make a new one.
     * @param uniqueName Same name used to help identify the created fs.
     * @return A new fs instance if we are up on DistributeFileSystem.
     * @throws IOException
     */

    @Override
    protected void handleReportForDutyResponse(MapWritable c) throws IOException {
      super.handleReportForDutyResponse(c);
      // Run this thread to shutdown our filesystem on way out.
      this.shutdownThread = new SingleFileSystemShutdownThread(getFileSystem());
    }

    @Override
    public void run() {
      try {
        this.user.runAs(new PrivilegedAction<Object>(){
          public Object run() {
            runRegionServer();
            return null;
          }
        });
      } catch (Throwable t) {
        LOG.error("Exception in run", t);
      } finally {
        // Run this on the way out.
        if (this.shutdownThread != null) {
          this.shutdownThread.start();
          Threads.shutdown(this.shutdownThread, 30000);
        }
      }
    }

    private void runRegionServer() {
      super.run();
    }

    @Override
    public void kill() {
      super.kill();
    }

    public void abort(final String reason, final Throwable cause) {
      this.user.runAs(new PrivilegedAction<Object>() {
        public Object run() {
          abortRegionServer(reason, cause);
          return null;
        }
      });
    }

    private void abortRegionServer(String reason, Throwable cause) {
      super.abort(reason, cause);
    }
  }

  /**
   * Alternate shutdown hook.
   * Just shuts down the passed fs, not all as default filesystem hook does.
   */
  static class SingleFileSystemShutdownThread extends Thread {
    private final FileSystem fs;
    SingleFileSystemShutdownThread(final FileSystem fs) {
      super("Shutdown of " + fs);
      this.fs = fs;
    }
    @Override
    public void run() {
      try {
        LOG.info("Hook closing fs=" + this.fs);
        this.fs.close();
      } catch (NullPointerException npe) {
        LOG.debug("Need to fix these: " + npe.toString());
      } catch (IOException e) {
        LOG.warn("Running hook", e);
      }
    }
  }

  private void init(final int nMasterNodes, final int nRegionNodes)
  throws IOException, InterruptedException {
    try {
      // start up a LocalHBaseCluster
      hbaseCluster = new LocalHBaseCluster(conf, nMasterNodes, 0,
          MiniHBaseCluster.MiniHBaseClusterMaster.class,
          MiniHBaseCluster.MiniHBaseClusterRegionServer.class);

      // manually add the regionservers as other users
      for (int i=0; i<nRegionNodes; i++) {
        Configuration rsConf = HBaseConfiguration.create(conf);
        User user = HBaseTestingUtility.getDifferentUser(rsConf,
            ".hfs."+index++);
        hbaseCluster.addRegionServer(rsConf, i, user);
      }

      hbaseCluster.startup();
    } catch (IOException e) {
      shutdown();
      throw e;
    } catch (Throwable t) {
      LOG.error("Error starting cluster", t);
      shutdown();
      throw new IOException("Shutting down", t);
    }
  }

  /**
   * Starts a region server thread running
   *
   * @throws IOException
   * @return New RegionServerThread
   */
  public JVMClusterUtil.RegionServerThread startRegionServer()
      throws IOException {
    final Configuration newConf = HBaseConfiguration.create(conf);
    User rsUser =
        HBaseTestingUtility.getDifferentUser(newConf, ".hfs."+index++);
    JVMClusterUtil.RegionServerThread t =  null;
    try {
      t = hbaseCluster.addRegionServer(
          newConf, hbaseCluster.getRegionServers().size(), rsUser);
      t.start();
      t.waitForServerOnline();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted executing UserGroupInformation.doAs()", ie);
    }
    return t;
  }

  /**
   * Cause a region server to exit doing basic clean up only on its way out.
   * @param serverNumber  Used as index into a list.
   */
  public String abortRegionServer(int serverNumber) {
    HRegionServer server = getRegionServer(serverNumber);
    LOG.info("Aborting " + server.toString());
    server.abort("Aborting for tests", new Exception("Trace info"));
    return server.toString();
  }

  /**
   * Shut down the specified region server cleanly
   *
   * @param serverNumber  Used as index into a list.
   * @return the region server that was stopped
   */
  public JVMClusterUtil.RegionServerThread stopRegionServer(int serverNumber) {
    return stopRegionServer(serverNumber, true);
  }

  /**
   * Shut down the specified region server cleanly
   *
   * @param serverNumber  Used as index into a list.
   * @param shutdownFS True is we are to shutdown the filesystem as part of this
   * regionserver's shutdown.  Usually we do but you do not want to do this if
   * you are running multiple regionservers in a test and you shut down one
   * before end of the test.
   * @return the region server that was stopped
   */
  public JVMClusterUtil.RegionServerThread stopRegionServer(int serverNumber,
      final boolean shutdownFS) {
    JVMClusterUtil.RegionServerThread server =
      hbaseCluster.getRegionServers().get(serverNumber);
    LOG.info("Stopping " + server.toString());
    server.getRegionServer().stop("Stopping rs " + serverNumber);
    return server;
  }

  /**
   * Wait for the specified region server to stop. Removes this thread from list
   * of running threads.
   * @param serverNumber
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(final int serverNumber) {
    return this.hbaseCluster.waitOnRegionServer(serverNumber);
  }


  /**
   * Starts a master thread running
   *
   * @throws IOException
   * @return New RegionServerThread
   */
  public JVMClusterUtil.MasterThread startMaster() throws IOException {
    Configuration c = HBaseConfiguration.create(conf);
    User user =
        HBaseTestingUtility.getDifferentUser(c, ".hfs."+index++);

    JVMClusterUtil.MasterThread t = null;
    try {
      t = hbaseCluster.addMaster(c, hbaseCluster.getMasters().size(), user);
      t.start();
      t.waitForServerOnline();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted executing UserGroupInformation.doAs()", ie);
    }
    return t;
  }

  /**
   * @return Returns the rpc address actually used by the currently active
   * master server, because the supplied port is not necessarily the actual port
   * used.
   */
  public HServerAddress getHMasterAddress() {
    return this.hbaseCluster.getActiveMaster().getMasterAddress();
  }

  /**
   * Returns the current active master, if available.
   * @return the active HMaster, null if none is active.
   */
  public HMaster getMaster() {
    return this.hbaseCluster.getActiveMaster();
  }

  /**
   * Returns the master at the specified index, if available.
   * @return the active HMaster, null if none is active.
   */
  public HMaster getMaster(final int serverNumber) {
    return this.hbaseCluster.getMaster(serverNumber);
  }

  /**
   * Cause a master to exit without shutting down entire cluster.
   * @param serverNumber  Used as index into a list.
   */
  public String abortMaster(int serverNumber) {
    HMaster server = getMaster(serverNumber);
    LOG.info("Aborting " + server.toString());
    server.abort("Aborting for tests", new Exception("Trace info"));
    return server.toString();
  }

  /**
   * Shut down the specified master cleanly
   *
   * @param serverNumber  Used as index into a list.
   * @return the region server that was stopped
   */
  public JVMClusterUtil.MasterThread stopMaster(int serverNumber) {
    return stopMaster(serverNumber, true);
  }

  /**
   * Shut down the specified master cleanly
   *
   * @param serverNumber  Used as index into a list.
   * @param shutdownFS True is we are to shutdown the filesystem as part of this
   * master's shutdown.  Usually we do but you do not want to do this if
   * you are running multiple master in a test and you shut down one
   * before end of the test.
   * @return the master that was stopped
   */
  public JVMClusterUtil.MasterThread stopMaster(int serverNumber,
      final boolean shutdownFS) {
    JVMClusterUtil.MasterThread server =
      hbaseCluster.getMasters().get(serverNumber);
    LOG.info("Stopping " + server.toString());
    server.getMaster().stop("Stopping master " + serverNumber);
    return server;
  }

  /**
   * Wait for the specified master to stop. Removes this thread from list
   * of running threads.
   * @param serverNumber
   * @return Name of master that just went down.
   */
  public String waitOnMaster(final int serverNumber) {
    return this.hbaseCluster.waitOnMaster(serverNumber);
  }

  /**
   * Blocks until there is an active master and that master has completed
   * initialization.
   *
   * @return true if an active master becomes available.  false if there are no
   *         masters left.
   * @throws InterruptedException
   */
  public boolean waitForActiveAndReadyMaster() throws InterruptedException {
    List<JVMClusterUtil.MasterThread> mts;
    while ((mts = getMasterThreads()).size() > 0) {
      for (JVMClusterUtil.MasterThread mt : mts) {
        if (mt.getMaster().isActiveMaster() && mt.getMaster().isInitialized()) {
          return true;
        }
      }
      Thread.sleep(200);
    }
    return false;
  }

  /**
   * @return List of master threads.
   */
  public List<JVMClusterUtil.MasterThread> getMasterThreads() {
    return this.hbaseCluster.getMasters();
  }

  /**
   * @return List of live master threads (skips the aborted and the killed)
   */
  public List<JVMClusterUtil.MasterThread> getLiveMasterThreads() {
    return this.hbaseCluster.getLiveMasters();
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   */
  public void join() {
    this.hbaseCluster.join();
  }

  /**
   * Shut down the mini HBase cluster
   * @throws IOException
   */
  public void shutdown() throws IOException {
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
    }
    HConnectionManager.deleteAllConnections(false);
  }

  /**
   * Call flushCache on all regions on all participating regionservers.
   * @throws IOException
   */
  public void flushcache() throws IOException {
    for (JVMClusterUtil.RegionServerThread t:
        this.hbaseCluster.getRegionServers()) {
      for(HRegion r: t.getRegionServer().getOnlineRegionsLocalContext()) {
        r.flushcache();
      }
    }
  }

  /**
   * Call flushCache on all regions of the specified table.
   * @throws IOException
   */
  public void flushcache(byte [] tableName) throws IOException {
    for (JVMClusterUtil.RegionServerThread t:
        this.hbaseCluster.getRegionServers()) {
      for(HRegion r: t.getRegionServer().getOnlineRegionsLocalContext()) {
        if(Bytes.equals(r.getTableDesc().getName(), tableName)) {
          r.flushcache();
        }
      }
    }
  }

  /**
   * @return List of region server threads.
   */
  public List<JVMClusterUtil.RegionServerThread> getRegionServerThreads() {
    return this.hbaseCluster.getRegionServers();
  }

  /**
   * @return List of live region server threads (skips the aborted and the killed)
   */
  public List<JVMClusterUtil.RegionServerThread> getLiveRegionServerThreads() {
    return this.hbaseCluster.getLiveRegionServers();
  }

  /**
   * Grab a numbered region server of your choice.
   * @param serverNumber
   * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    return hbaseCluster.getRegionServer(serverNumber);
  }

  public List<HRegion> getRegions(byte[] tableName) {
    List<HRegion> ret = new ArrayList<HRegion>();
    for (JVMClusterUtil.RegionServerThread rst : getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      for (HRegion region : hrs.getOnlineRegionsLocalContext()) {
        if (Bytes.equals(region.getTableDesc().getName(), tableName)) {
          ret.add(region);
        }
      }
    }
    return ret;
  }

  /**
   * @return Index into List of {@link MiniHBaseCluster#getRegionServerThreads()}
   * of HRS carrying regionName. Returns -1 if none found.
   */
  public int getServerWithMeta() {
    return getServerWith(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
  }

  /**
   * Get the location of the specified region
   * @param regionName Name of the region in bytes
   * @return Index into List of {@link MiniHBaseCluster#getRegionServerThreads()}
   * of HRS carrying .META.. Returns -1 if none found.
   */
  public int getServerWith(byte[] regionName) {
    int index = -1;
    int count = 0;
    for (JVMClusterUtil.RegionServerThread rst: getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      HRegion metaRegion =
        hrs.getOnlineRegion(regionName);
      if (metaRegion != null) {
        index = count;
        break;
      }
      count++;
    }
    return index;
  }

  /**
   * Add an exception to send when a region server checks back in
   * @param serverNumber Which server to send it to
   * @param ex The exception that will be sent
   * @throws IOException
   */
  public void addExceptionToSendRegionServer(final int serverNumber,
      IOException ex) throws IOException {
    MiniHBaseClusterRegionServer hrs =
      (MiniHBaseClusterRegionServer)getRegionServer(serverNumber);
    addExceptionToSendRegionServer(hrs, ex);
  }

  /**
   * Add an exception to send when a region server checks back in
   * @param hrs Which server to send it to
   * @param ex The exception that will be sent
   * @throws IOException
   */
  public void addExceptionToSendRegionServer(
      final MiniHBaseClusterRegionServer hrs, IOException ex)
      throws IOException {
    ((MiniHBaseClusterMaster)getMaster()).addException(hrs.getHServerInfo(),ex);
  }

  /**
   * Add a message to include in the responses send a regionserver when it
   * checks back in.
   * @param serverNumber Which server to send it to.
   * @param msg The MESSAGE
   * @throws IOException
   */
  public void addMessageToSendRegionServer(final int serverNumber,
    final HMsg msg)
  throws IOException {
    MiniHBaseClusterRegionServer hrs =
      (MiniHBaseClusterRegionServer)getRegionServer(serverNumber);
    addMessageToSendRegionServer(hrs, msg);
  }

  /**
   * Add a message to include in the responses send a regionserver when it
   * checks back in.
   * @param hrs Which region server.
   * @param msg The MESSAGE
   * @throws IOException
   */
  public void addMessageToSendRegionServer(final MiniHBaseClusterRegionServer hrs,
    final HMsg msg)
  throws IOException {
    ((MiniHBaseClusterMaster)getMaster()).addMessage(hrs.getHServerInfo(), msg);
  }

  /**
   * Counts the total numbers of regions being served by the currently online
   * region servers by asking each how many regions they have.  Does not look
   * at META at all.  Count includes catalog tables.
   * @return number of regions being served by all region servers
   */
  public long countServedRegions() {
    long count = 0;
    for (JVMClusterUtil.RegionServerThread rst : getLiveRegionServerThreads()) {
      count += rst.getRegionServer().getNumberOfOnlineRegions();
    }
    return count;
  }
}