package org.apache.hadoop.yarn.health;

import java.util.HashMap;
import java.util.Map;

public class HealthReport {
    public enum WorkState {
        UNCHECKED, NORMAL, IDLE, BUSY
    }
    private String name;
    private boolean healthy;
    private WorkState workState;
    private long updateTime;
    private String diagnostics;
    private Map<String, Object> metrics;

    public static HealthReport getInstance(String name, boolean healthy, WorkState workState, String diagnostics) {
        HealthReport report = new HealthReport();
        report.name = name;
        report.healthy = healthy;
        report.workState = workState;
        report.diagnostics = diagnostics;
        report.updateTime = System.currentTimeMillis();
        report.metrics = new HashMap<>();
        return report;
    }

    public static HealthReport getInstance(String name, boolean healthy, WorkState workState) {
        return getInstance(name, healthy, workState, "");
    }

    public static HealthReport getInstance(String name, boolean healthy) {
        return getInstance(name, healthy, WorkState.UNCHECKED);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    public WorkState getWorkState() {
        return workState;
    }

    public void setWorkState(WorkState workState) {
        this.workState = workState;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    public void putMetrics(String key, Object value) {
        this.metrics.put(key, value);
    }

    @Override
    public String toString() {
        return "HealthReport{" +
                "name='" + name + '\'' +
                ", healthy=" + healthy +
                ", workState=" + workState +
                ", updateTime=" + updateTime +
                ", diagnostics='" + diagnostics + '\'' +
                ", metrics=" + metrics +
                '}';
    }
}
