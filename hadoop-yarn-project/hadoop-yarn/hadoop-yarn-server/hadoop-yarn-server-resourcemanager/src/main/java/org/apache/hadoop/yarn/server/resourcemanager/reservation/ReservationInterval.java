package org.apache.hadoop.yarn.server.resourcemanager.reservation;

/**
 * This represents the time duration of the reservation
 * 
 */
public class ReservationInterval implements Comparable<ReservationInterval> {

  private final long startTime;

  private final long endTime;

  public ReservationInterval(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * Get the start time of the reservation interval
   * 
   * @return the startTime
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Get the end time of the reservation interval
   * 
   * @return the endTime
   */
  public long getEndTime() {
    return endTime;
  }

  /**
   * Returns whether the interval is active at the specified instant of time
   * 
   * @param tick the instance of the time to check
   * @return true if active, false otherwise
   */
  public boolean isOverlap(long tick) {
    return (startTime <= tick && tick <= endTime);
  }

  @Override
  public int compareTo(ReservationInterval anotherInterval) {
    long diff = 0;
    if (startTime == anotherInterval.getStartTime()) {
      diff = endTime - anotherInterval.getEndTime();
    } else {
      diff = startTime - anotherInterval.getStartTime();
    }
    if (diff < 0) {
      return -1;
    } else if (diff > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  public String toString() {
    return "[" + startTime + ", " + endTime + "]";
  }

}
