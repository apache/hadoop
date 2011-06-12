package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Static class which encapsulates the Node health
 * related fields.
 * 
 */
public class NodeHealthStatus implements
  org.apache.hadoop.yarn.server.api.records.NodeHealthStatus, Writable {

  private boolean isNodeHealthy;
  private String healthReport;
  private long lastHealthReportTime;

  public NodeHealthStatus(boolean isNodeHealthy, String healthReport,
      long lastReported) {
    this.isNodeHealthy = isNodeHealthy;
    this.healthReport = healthReport;
    this.lastHealthReportTime = lastReported;
  }
  
  public NodeHealthStatus() {
    this.isNodeHealthy = true;
    this.healthReport = "";
    this.lastHealthReportTime = System.currentTimeMillis();
  }

  /**
   * Sets whether or not a task tracker is healthy or not, based on the
   * output from the node health script.
   * 
   * @param isNodeHealthy
   */
  @Override
  public void setIsNodeHealthy(boolean isNodeHealthy) {
    this.isNodeHealthy = isNodeHealthy;
  }

  /**
   * Returns if node is healthy or not based on result from node health
   * script.
   * 
   * @return true if the node is healthy.
   */
  @Override
  public boolean getIsNodeHealthy() {
    return isNodeHealthy;
  }

  /**
   * Sets the health report based on the output from the health script.
   * 
   * @param healthReport
   *          String listing cause of failure.
   */
  @Override
  public void setHealthReport(String healthReport) {
    this.healthReport = healthReport;
  }

  /**
   * Returns the health report of the node if any, The health report is
   * only populated when the node is not healthy.
   * 
   * @return health report of the node if any
   */
  public String getHealthReport() {
    return healthReport.toString();
  }

  /**
   * Sets when the TT got its health information last 
   * from node health monitoring service.
   * 
   * @param lastReported last reported time by node 
   * health script
   */
  public void setLastHealthReportTime(long lastReported) {
    this.lastHealthReportTime = lastReported;
  }

  /**
   * Gets time of most recent node health update.
   * 
   * @return time stamp of most recent health update.
   */
  @Override
  public long getLastHealthReportTime() {
    return lastHealthReportTime;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    isNodeHealthy = in.readBoolean();
    healthReport = Text.readString(in);
    lastHealthReportTime = in.readLong();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(isNodeHealthy);
    Text.writeString(out, healthReport.toString());
    out.writeLong(lastHealthReportTime);
  }
  
}