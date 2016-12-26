package org.apache.hadoop.yarn.applications.tensorflow;

/**
 * Created by muzhongz on 16-12-21.
 */
public class TFServerAddress {
    private String address;
    private int port;
    private String jobName; /* worker or ps */
    private int taskIndex;
    private ClusterSpec clusterSpec;

    protected TFServerAddress(ClusterSpec cluster, String address, int port, String jobName, int taskIndex) {
        this.setClusterSpec(cluster);
        this.setAddress(address);
        this.setPort(port);
        this.setJobName(jobName);
        this.setTaskIndex(taskIndex);
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public void setTaskIndex(int taskIndex) {
        this.taskIndex = taskIndex;
    }

    public ClusterSpec getClusterSpec() {
        return clusterSpec;
    }

    public void setClusterSpec(ClusterSpec clusterSpec) {
        this.clusterSpec = clusterSpec;
    }
}
