package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * TaskTracker client for system tests. Assumption of the class is that the
 * configuration key is set for the configuration key : {@code
 * mapred.task.tracker.report.address}is set, only the port portion of the
 * address is used.
 */
public class TTClient extends MRDaemonClient<TTProtocol> {

  TTProtocol proxy;

  public TTClient(Configuration conf, RemoteProcess daemon) 
      throws IOException {
    super(conf, daemon);
  }

  @Override
  public synchronized void connect() throws IOException {
    if (isConnected()) {
      return;
    }
    String sockAddrStr = getConf()
        .get("mapred.task.tracker.report.address");
    if (sockAddrStr == null) {
      throw new IllegalArgumentException(
          "TaskTracker report address is not set");
    }
    String[] splits = sockAddrStr.split(":");
    if (splits.length != 2) {
      throw new IllegalArgumentException(
          "TaskTracker report address not correctly configured");
    }
    String port = splits[1];
    String sockAddr = getHostName() + ":" + port;
    InetSocketAddress bindAddr = NetUtils.createSocketAddr(sockAddr);
    proxy = (TTProtocol) RPC.getProxy(TTProtocol.class, TTProtocol.versionID,
        bindAddr, getConf());
    setConnected(true);
  }

  @Override
  public synchronized void disconnect() throws IOException {
    RPC.stopProxy(proxy);
  }

  @Override
  public synchronized TTProtocol getProxy() {
    return proxy;
  }

  /**
   * Gets the last sent status to the {@link JobTracker}. <br/>
   * 
   * @return the task tracker status.
   * @throws IOException
   */
  public TaskTrackerStatus getStatus() throws IOException {
    return getProxy().getStatus();
  }

}
