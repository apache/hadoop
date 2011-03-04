package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.AbstractDaemonClient;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * Base class for JobTracker and TaskTracker clients.
 */
public abstract class MRDaemonClient<PROXY extends DaemonProtocol> 
    extends AbstractDaemonClient<PROXY>{

  public MRDaemonClient(Configuration conf, RemoteProcess process)
      throws IOException {
    super(conf, process);
  }

  public String[] getMapredLocalDirs() throws IOException {
    return getProxy().getDaemonConf().getStrings("mapred.local.dir");
  }

  public String getLogDir() throws IOException {
    return getProcessInfo().getSystemProperties().get("hadoop.log.dir");
  }
}
