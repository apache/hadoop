package org.apache.hadoop.test.system.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.system.process.ClusterProcessManager.ClusterType;

/**
 * Factory to create ClusterProcessManager handle.
 */
public class ClusterProcessManagerFactory {

  /**
   * Factory method to create the {@link ClusterProcessManager} based on the
   * {@code ClusterProcessManager.IMPL_CLASS} value. <br/>
   * 
   * @param t type of the cluster to be managed by the instance.
   * @param conf the configuration required by the instance for 
   * management of cluster.
   * @return instance of the cluster to be used for management.
   * 
   * @throws Exception
   */
  public static ClusterProcessManager createInstance(ClusterType t,
      Configuration conf) throws Exception {
    String implKlass = conf.get(ClusterProcessManager.IMPL_CLASS, System
        .getProperty(ClusterProcessManager.IMPL_CLASS));
    if (implKlass == null || implKlass.isEmpty()) {
      implKlass = HadoopDaemonRemoteCluster.class.getName();
    }
    Class<ClusterProcessManager> klass = (Class<ClusterProcessManager>) Class
        .forName(implKlass);
    ClusterProcessManager k = klass.newInstance();
    k.init(t, conf);
    return k;
  }
}
