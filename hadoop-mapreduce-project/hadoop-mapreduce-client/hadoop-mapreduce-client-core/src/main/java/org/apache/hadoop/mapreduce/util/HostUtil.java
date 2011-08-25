package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class HostUtil {

  /**
   * Construct the taskLogUrl
   * @param taskTrackerHostName
   * @param httpPort
   * @param taskAttemptID
   * @return the taskLogUrl
   */
  public static String getTaskLogUrl(String taskTrackerHostName,
      String httpPort, String taskAttemptID) {
    return ("http://" + taskTrackerHostName + ":" + httpPort
        + "/tasklog?attemptid=" + taskAttemptID);
  }

  public static String convertTrackerNameToHostName(String trackerName) {
    // Ugly!
    // Convert the trackerName to its host name
    int indexOfColon = trackerName.indexOf(":");
    String trackerHostName = (indexOfColon == -1) ? 
      trackerName : 
      trackerName.substring(0, indexOfColon);
    return trackerHostName.substring("tracker_".length());
  }

}
