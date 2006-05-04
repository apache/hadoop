package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskTracker;

/**
 * Reducer that accumulates values based on their type.
 * <p>
 * The type is specified in the key part of the key-value pair 
 * as a prefix to the key in the following way
 * <p>
 * <tt>type:key</tt>
 * <p>
 * The values are accumulated according to the types:
 * <ul>
 * <li><tt>s:</tt> - string, concatenate</li>
 * <li><tt>f:</tt> - float, summ</li>
 * <li><tt>l:</tt> - long, summ</li>
 * </ul>
 * 
 * @author Konstantin Shvachko
 */
public class AccumulatingReducer extends MapReduceBase implements Reducer {
  protected String hostName;
  
  public AccumulatingReducer () {
    TaskTracker.LOG.info("Starting AccumulatingReducer !!!");
    try {
      hostName = java.net.InetAddress.getLocalHost().getHostName();
    } catch(Exception e) {
      hostName = "localhost";
    }
    TaskTracker.LOG.info("Starting AccumulatingReducer on " + hostName);
  }
  
  public void reduce( WritableComparable key, 
                      Iterator values,
                      OutputCollector output, 
                      Reporter reporter
                      ) throws IOException {
    String field = ((UTF8) key).toString();

    reporter.setStatus("starting " + field + " ::host = " + hostName);

    // concatenate strings
    if (field.startsWith("s:")) {
      String sSum = "";
      while (values.hasNext())
        sSum += ((UTF8) values.next()).toString() + ";";
      output.collect(key, new UTF8(sSum));
      reporter.setStatus("finished " + field + " ::host = " + hostName);
      return;
    }
    // sum long values
    if (field.startsWith("f:")) {
      float fSum = 0;
      while (values.hasNext())
        fSum += Float.parseFloat(((UTF8) values.next()).toString());
      output.collect(key, new UTF8(String.valueOf(fSum)));
      reporter.setStatus("finished " + field + " ::host = " + hostName);
      return;
    }
    // sum long values
    if (field.startsWith("l:")) {
      long lSum = 0;
      while (values.hasNext()) {
        lSum += Long.parseLong(((UTF8) values.next()).toString());
      }
      output.collect(key, new UTF8(String.valueOf(lSum)));
    }
    reporter.setStatus("finished " + field + " ::host = " + hostName);
  }
}
