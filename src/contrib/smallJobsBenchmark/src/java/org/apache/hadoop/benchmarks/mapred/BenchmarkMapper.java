package org.apache.hadoop.benchmarks.mapred;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * takes inpt format as text lines, runs some processing on it and 
 * writes out data as text again. 
 * 
 * @author sanjaydahiya
 *
 */
public class BenchmarkMapper extends MapReduceBase implements Mapper {
  
  public void map(WritableComparable key, Writable value,
      OutputCollector output, Reporter reporter) throws IOException {
    
    String line = value.toString();
    output.collect(new UTF8(process(line)), new UTF8(""));		
  }
  
  public String process(String line){
    return line ; 
  }
  
}
