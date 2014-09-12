package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

public class QueueEntitlement {

  private float capacity;
  private float maxCapacity;

  public QueueEntitlement(float capacity, float maxCapacity){
    this.setCapacity(capacity);
    this.maxCapacity = maxCapacity;
   }

  public float getMaxCapacity() {
    return maxCapacity;
  }

  public void setMaxCapacity(float maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  public float getCapacity() {
    return capacity;
  }

  public void setCapacity(float capacity) {
    this.capacity = capacity;
  }
}
