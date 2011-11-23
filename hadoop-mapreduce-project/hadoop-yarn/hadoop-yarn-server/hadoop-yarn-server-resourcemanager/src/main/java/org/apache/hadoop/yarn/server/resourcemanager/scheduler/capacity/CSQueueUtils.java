package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

class CSQueueUtils {
  
  public static void checkMaxCapacity(String queueName, 
      float capacity, float maximumCapacity) {
    if (maximumCapacity != CapacitySchedulerConfiguration.UNDEFINED && 
        maximumCapacity < capacity) {
      throw new IllegalArgumentException(
          "Illegal call to setMaxCapacity. " +
          "Queue '" + queueName + "' has " +
          "capacity (" + capacity + ") greater than " + 
          "maximumCapacity (" + maximumCapacity + ")" );
    }
  }
  
}
