package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Filter that returns only cells whose timestamp (version) is
 * in the specified list of timestamps (versions).
 * <p>
 * Note: Use of this filter overrides any time range/time stamp
 * options specified using {@link org.apache.hadoop.hbase.client.Get#setTimeRange(long, long)},
 * {@link org.apache.hadoop.hbase.client.Scan#setTimeRange(long, long)}, {@link org.apache.hadoop.hbase.client.Get#setTimeStamp(long)},
 * or {@link org.apache.hadoop.hbase.client.Scan#setTimeStamp(long)}.
 */
public class TimestampsFilter extends FilterBase {

  TreeSet<Long> timestamps;

  // Used during scans to hint the scan to stop early
  // once the timestamps fall below the minTimeStamp.
  long minTimeStamp = Long.MAX_VALUE;

  /**
   * Used during deserialization. Do not use otherwise.
   */
  public TimestampsFilter() {
    super();
  }

  /**
   * Constructor for filter that retains only those
   * cells whose timestamp (version) is in the specified
   * list of timestamps.
   *
   * @param timestamps
   */
  public TimestampsFilter(List<Long> timestamps) {
    this.timestamps = new TreeSet<Long>(timestamps);
    init();
  }

  /**
   * @return the list of timestamps
   */
  public List<Long> getTimestamps() {
    List<Long> list = new ArrayList<Long>(timestamps.size());
    list.addAll(timestamps);
    return list;
  }

  private void init() {
    if (this.timestamps.size() > 0) {
      minTimeStamp = this.timestamps.first();
    }
  }

  /**
   * Gets the minimum timestamp requested by filter.
   * @return  minimum timestamp requested by filter.
   */
  public long getMin() {
    return minTimeStamp;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    if (this.timestamps.contains(v.getTimestamp())) {
      return ReturnCode.INCLUDE;
    } else if (v.getTimestamp() < minTimeStamp) {
      // The remaining versions of this column are guaranteed
      // to be lesser than all of the other values.
      return ReturnCode.NEXT_COL;
    }
    return ReturnCode.SKIP;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numTimestamps = in.readInt();
    this.timestamps = new TreeSet<Long>();
    for (int idx = 0; idx < numTimestamps; idx++) {
      this.timestamps.add(in.readLong());
    }
    init();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int numTimestamps = this.timestamps.size();
    out.writeInt(numTimestamps);
    for (Long timestamp : this.timestamps) {
      out.writeLong(timestamp);
    }
  }
}
