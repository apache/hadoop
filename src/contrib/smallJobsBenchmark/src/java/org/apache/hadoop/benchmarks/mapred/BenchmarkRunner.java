package org.apache.hadoop.benchmarks.mapred;


import org.apache.hadoop.util.ProgramDriver;
/**
 * Driver for benchmark. 
 * @author sanjaydahiya
 *
 */
public class BenchmarkRunner {
  
  public static void main(String argv[]){
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("smallJobsBenchmark", MultiJobRunner.class, 
      "A map/reduce benchmark that creates many small jobs");
      pgd.driver(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
  }
}
