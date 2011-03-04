package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.AbstractMasterSlaveCluster;
import org.apache.hadoop.test.system.process.ClusterProcessManager;
import org.apache.hadoop.test.system.process.ClusterProcessManagerFactory;
import org.apache.hadoop.test.system.process.RemoteProcess;
import org.apache.hadoop.test.system.process.ClusterProcessManager.ClusterType;

/**
 * Concrete MasterSlaveCluster representing a Map-Reduce cluster.
 * 
 */
public class MRCluster extends AbstractMasterSlaveCluster<JTClient, 
      TTClient> {

  private static final Log LOG = LogFactory.getLog(MRCluster.class);

  private MRCluster(Configuration conf, ClusterProcessManager rCluster)
      throws IOException {
    super(conf, rCluster);
  }

  /**
   * Creates an instance of the Map-Reduce cluster.<br/>
   * Example usage: <br/>
   * <code>
   * Configuration conf = new Configuration();<br/>
   * conf.set(ClusterProcessManager.IMPL_CLASS,
   * org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster.
   * class.getName())<br/>
   * conf.set(HadoopDaemonRemoteCluster.CONF_HADOOPHOME,
   * "/path");<br/>
   * conf.set(HadoopDaemonRemoteCluster.CONF_HADOOPCONFDIR,
   * "/path");<br/>
   * MRCluster cluster = MRCluster.createCluster(conf);
   * </code>
   * 
   * @param conf
   *          contains all required parameter to create cluster.
   * @return a cluster instance to be managed.
   * @throws IOException
   * @throws Exception
   */
  public static MRCluster createCluster(Configuration conf) 
      throws IOException, Exception {
    return new MRCluster(conf, ClusterProcessManagerFactory.createInstance(
        ClusterType.MAPRED, conf));
  }

  @Override
  protected JTClient createMaster(RemoteProcess masterDaemon)
      throws IOException {
    return new JTClient(getConf(), masterDaemon);
  }

  @Override
  protected TTClient createSlave(RemoteProcess slaveDaemon) 
      throws IOException {
    return new TTClient(getConf(), slaveDaemon);
  }

  @Override
  public void ensureClean() throws IOException {
    //TODO: ensure that no jobs/tasks are running
    //restart the cluster if cleanup fails
  }
}
