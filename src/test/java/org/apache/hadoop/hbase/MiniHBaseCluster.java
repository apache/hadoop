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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class creates a single process HBase cluster.
 * each server.  The master uses the 'default' FileSystem.  The RegionServers,
 * if we are running on DistributedFilesystem, create a FileSystem instance
 * each and will close down their instance on the way out.
 */
public class MiniHBaseCluster implements HConstants {
  static final Log LOG = LogFactory.getLog(MiniHBaseCluster.class.getName());
  private Configuration conf;
  public LocalHBaseCluster hbaseCluster;
  // Cache this.  For some reason only works first time I get it.  TODO: Figure
  // out why.
  private final static UserGroupInformation UGI;
  static {
    UGI = UserGroupInformation.getCurrentUGI();
  }

  /**
   * Start a MiniHBaseCluster.
   * @param conf Configuration to be used for cluster
   * @param numRegionServers initial number of region servers to start.
   * @throws IOException
   */
  public MiniHBaseCluster(Configuration conf, int numRegionServers)
  throws IOException {
    this.conf = conf;
    conf.set(MASTER_PORT, "0");
    init(numRegionServers);
  }

  /**
   * Override Master so can add inject behaviors testing.
   */
  public static class MiniHBaseClusterMaster extends HMaster {
    private final Map<HServerInfo, List<HMsg>> messages =
      new ConcurrentHashMap<HServerInfo, List<HMsg>>();

    public MiniHBaseClusterMaster(final Configuration conf)
    throws IOException {
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

    @Override
    protected HMsg[] adornRegionServerAnswer(final HServerInfo hsi,
        final HMsg[] msgs) {
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
    private static int index = 0;
    private Thread shutdownThread = null;

    public MiniHBaseClusterRegionServer(Configuration conf)
        throws IOException {
      super(setDifferentUser(conf));
    }

    /*
     * @param c
     * @param currentfs We return this if we did not make a new one.
     * @param uniqueName Same name used to help identify the created fs.
     * @return A new fs instance if we are up on DistributeFileSystem.
     * @throws IOException
     */
    private static Configuration setDifferentUser(final Configuration c)
    throws IOException {
      FileSystem currentfs = FileSystem.get(c);
      if (!(currentfs instanceof DistributedFileSystem)) return c;
      // Else distributed filesystem.  Make a new instance per daemon.  Below
      // code is taken from the AppendTestUtil over in hdfs.
      Configuration c2 = new Configuration(c);
      String username = UGI.getUserName() + ".hrs." + index++;
      UnixUserGroupInformation.saveToConf(c2,
        UnixUserGroupInformation.UGI_PROPERTY_NAME,
        new UnixUserGroupInformation(username, new String[]{"supergroup"}));
      return c2;
    }
    
    @Override
    protected void init(MapWritable c) throws IOException {
      super.init(c);
      // Run this thread to shutdown our filesystem on way out.
      this.shutdownThread = new SingleFileSystemShutdownThread(getFileSystem());
    }

    @Override
    public void run() {
      try {
        super.run();
      } finally {
        // Run this on the way out.
        if (this.shutdownThread != null) {
          this.shutdownThread.start();
          Threads.shutdown(this.shutdownThread, 30000);
        }
      }
    }
 
    public void kill() {
      super.kill();
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
      } catch (IOException e) {
        LOG.warn("Running hook", e);
      }
    }
  }

  private void init(final int nRegionNodes) throws IOException {
    try {
      // start up a LocalHBaseCluster
      hbaseCluster = new LocalHBaseCluster(conf, nRegionNodes,
          MiniHBaseCluster.MiniHBaseClusterMaster.class,
          MiniHBaseCluster.MiniHBaseClusterRegionServer.class);
      hbaseCluster.startup();
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  /**
   * Starts a region server thread running
   *
   * @throws IOException
   * @return New RegionServerThread
   */
  public JVMClusterUtil.RegionServerThread startRegionServer() throws IOException {
    JVMClusterUtil.RegionServerThread t = this.hbaseCluster.addRegionServer();
    t.start();
    t.waitForServerOnline();
    return t;
  }

  /**
   * @return Returns the rpc address actually used by the master server, because
   * the supplied port is not necessarily the actual port used.
   */
  public HServerAddress getHMasterAddress() {
    return this.hbaseCluster.getMaster().getMasterAddress();
  }

  /**
   * @return the HMaster
   */
  public HMaster getMaster() {
    return this.hbaseCluster.getMaster();
  }

  /**
   * Cause a region server to exit doing basic clean up only on its way out.
   * @param serverNumber  Used as index into a list.
   */
  public String abortRegionServer(int serverNumber) {
    HRegionServer server = getRegionServer(serverNumber);
    LOG.info("Aborting " + server.toString());
    server.abort();
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
    server.getRegionServer().stop();
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
      for(HRegion r: t.getRegionServer().getOnlineRegions()) {
        r.flushcache();
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
      for (HRegion region : hrs.getOnlineRegions()) {
        if (Bytes.equals(region.getTableDesc().getName(), tableName)) {
          ret.add(region);
        }
      }
    }
    return ret;
  }

  /**
   * @return Index into List of {@link MiniHBaseCluster#getRegionServerThreads()}
   * of HRS carrying .META.  Returns -1 if none found.
   */
  public int getServerWithMeta() {
    int index = -1;
    int count = 0;
    for (JVMClusterUtil.RegionServerThread rst: getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      HRegion metaRegion =
        hrs.getOnlineRegion(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      if (metaRegion != null) {
        index = count;
        break;
      }
      count++;
    }
    return index;
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
}
