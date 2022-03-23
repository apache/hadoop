package org.apache.hadoop.util;

public class NodeResource {

    float cpuUsage;
    float ioUsage;
    float memoryUsage;

    public NodeResource(float cpuUsage, float ioUsage, float memoryUsage) {
        this.cpuUsage = cpuUsage;
        this.ioUsage = ioUsage;
        this.memoryUsage = memoryUsage;
    }

    public float getCpuUsage() {
        return this.cpuUsage;
    }

    public float getIoUsage() {
        return this.ioUsage;
    }

    public float getMemoryUsage() {
        return this.memoryUsage;
    }

    public void setCpuUsage(float cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public void setIoUsage(float ioUsage) {
        this.ioUsage = ioUsage;
    }

    public void setMemoryUsage(float memoryUsage) {
        this.memoryUsage = memoryUsage;
    }
}
