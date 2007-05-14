package org.apache.hadoop.mapred;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.fs.*;

public class BigMapOutput {

  public static void main(String[] args) throws IOException {
    if (args.length != 4) { //input-dir should contain a huge file ( > 2GB)
      System.err.println("BigMapOutput " +
                       "-input <input-dir> -output <output-dir>");
      System.exit(1);
    } 
    Path bigMapInput = null;
    Path outputPath = null;
    for(int i=0; i < args.length; ++i) {
      if ("-input".equals(args[i])){
        bigMapInput = new Path(args[++i]);
      } else if ("-output".equals(args[i])){
        outputPath = new Path(args[++i]);
      }
    }
    Configuration defaults = new Configuration();
    FileSystem fs = FileSystem.get(defaults);
    
    JobConf jobConf = new JobConf(defaults, BigMapOutput.class);

    jobConf.setJobName("BigMapOutput");
    jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    jobConf.setInputPath(bigMapInput);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath);
    }
    jobConf.setOutputPath(outputPath);
    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(jobConf);
    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
      
  }
}
