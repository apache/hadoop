package org.apache.hadoop.hdfs.protocol;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;

/**********************************************************************
 * ReconfigurationProtocol is used by HDFS admin to reload configuration
 * for NN/DN without restarting them.
 **********************************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ReconfigurationProtocol {

  long versionID = 1L;

  /**
   * Asynchronously reload configuration on disk and apply changes.
   */
  void startReconfiguration() throws IOException;

  /**
   * Get the status of the previously issued reconfig task.
   * @see {@link org.apache.hadoop.conf.ReconfigurationTaskStatus}.
   */
  ReconfigurationTaskStatus getReconfigurationStatus() throws IOException;

  /**
   * Get a list of allowed properties for reconfiguration.
   */
  List<String> listReconfigurableProperties() throws IOException;
}
