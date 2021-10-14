package org.apache.hadoop.yarn.health;

@FunctionalInterface
public interface HealthReporter {
    HealthReport getHealthReport();
}
