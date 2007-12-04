package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;


/*
 * Subclass of StopRowFilter that filters rows > the stop row,
 * making it include up to the last row but no further.
 */
public class InclusiveStopRowFilter extends StopRowFilter{
  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public InclusiveStopRowFilter() {super();}

  /**
   * Constructor that takes a stopRowKey on which to filter
   * 
   * @param stopRowKey rowKey to filter on.
   */
  public InclusiveStopRowFilter(final Text stopRowKey) {
    super(stopRowKey);
  }
  
  public boolean filter(final Text rowKey) {
    if (rowKey == null) {
      if (this.stopRowKey == null) {
        return true;
      }
      return false;
    }    
    boolean result = this.stopRowKey.compareTo(rowKey) < 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filter result for rowKey: " + rowKey + ".  Result: " + 
        result);
    }
    return result;
  }
  
}