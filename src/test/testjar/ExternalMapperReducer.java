package testjar;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ExternalMapperReducer
  implements Mapper, Reducer {

  public void configure(JobConf job) {

  }

  public void close()
    throws IOException {

  }

  public void map(WritableComparable key, Writable value,
    OutputCollector output, Reporter reporter)
    throws IOException {
    
    if (value instanceof Text) {
      Text text = (Text)value;
      ExternalWritable ext = new ExternalWritable(text.toString());
      output.collect(ext, new IntWritable(1));
    }
  }

  public void reduce(WritableComparable key, Iterator values,
    OutputCollector output, Reporter reporter)
    throws IOException {
    
    int count = 0;
    while (values.hasNext()) {
      count++;
      values.next();
    }
    output.collect(key, new IntWritable(count));
  }
}
