package org.apache.hadoop.benchmarks.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author sanjaydahiya
 *
 */
public class BenchmarkReducer extends MapReduceBase implements Reducer {
  
  public void reduce(WritableComparable key, Iterator values,
      OutputCollector output, Reporter reporter) throws IOException {
    
    // ignore the key and write values to output
    while(values.hasNext()){
      output.collect(key, new UTF8(values.next().toString()));
    }
  }
  
  public String process(String line){
    return line ;
  }
}
