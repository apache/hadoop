package org.apache.hadoop.test.system.process;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to manage the remote processes in the master-slave cluster.
 */
public interface ClusterProcessManager {

  /**
   * The configuration key to specify the concrete implementation of the
   * {@link ClusterProcessManager} to be used by
   * {@link ClusterProcessManagerFactory}.
   */
  String IMPL_CLASS = "test.system.clusterprocessmanager.impl.class";

  /**
   * Enumeration used to specify the types of the clusters which are supported
   * by the concrete implementations of {@link ClusterProcessManager}.
   */
  public enum ClusterType {
    MAPRED, HDFS
  }
  
  /**
   * Initialization method to set cluster type and also pass the configuration
   * object which is required by the ClusterProcessManager to manage the 
   * cluster.<br/>
   * Configuration object should typically contain all the parameters which are 
   * required by the implementations.<br/>
   *  
   * @param t type of the cluster to be managed.
   * @param conf configuration containing values of the specific keys which 
   * are required by the implementation of the cluster process manger.
   * 
   * @throws Exception when initialization fails.
   */
  void init(ClusterType t, Configuration conf) throws Exception;

  /**
   * Getter for master daemon process for managing the master daemon.<br/>
   * 
   * @return master daemon process.
   */
  RemoteProcess getMaster();

  /**
   * Getter for slave daemon process for managing the slaves.<br/>
   * 
   * @return map of slave hosts to slave daemon process.
   */
  Map<String, RemoteProcess> getSlaves();

  /**
   * Method to start the cluster including all master and slaves.<br/>
   * 
   * @throws IOException if startup procedure fails.
   */
  void start() throws IOException;

  /**
   * Method to shutdown all the master and slaves.<br/>
   * 
   * @throws IOException if shutdown procedure fails.
   */
  void stop() throws IOException;

  /**
   * Method cleans the remote hosts' directories used by a deployed cluster
   * @throws IOException is thrown if cleaning process fails or terminates with
   * non-zero exit code
   */
  void cleanDirs() throws Exception;

  /**
   * Method (re)reploys cluster bits to the remote hosts
   * @throws IOException is thrown if cleaning process fails or terminates with
   * non-zero exit code
  */
  void deploy() throws IOException;
}
