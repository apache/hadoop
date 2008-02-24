
package org.apache.hadoop.hbase.regionserver;

/*
 * Data structure to hold result of a look at store file sizes.
 */
public class HStoreSize {
  final long aggregate;
  final long largest;
  boolean splitable;
  
  HStoreSize(final long a, final long l, final boolean s) {
    this.aggregate = a;
    this.largest = l;
    this.splitable = s;
  }
  
  public long getAggregate() {
    return this.aggregate;
  }
  
  public long getLargest() {
    return this.largest;
  }
  
  public boolean isSplitable() {
    return this.splitable;
  }
  
  public void setSplitable(final boolean s) {
    this.splitable = s;
  }
}