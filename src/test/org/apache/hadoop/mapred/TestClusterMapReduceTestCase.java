package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
import java.util.Iterator;
import java.util.Properties;

public class TestClusterMapReduceTestCase extends ClusterMapReduceTestCase {

  public static class EchoMap implements Mapper {

    public void configure(JobConf conf) {
    }

    public void close() {
    }

    public void map(WritableComparable key, Writable value,
                    OutputCollector collector, Reporter reporter) throws IOException {
      collector.collect(key, value);
    }
  }

  public static class EchoReduce implements Reducer {

    public void configure(JobConf conf) {
    }

    public void close() {
    }

    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector collector, Reporter reporter) throws IOException {
      while (values.hasNext()) {
        Writable value = (Writable) values.next();
        collector.collect(key, value);
      }
    }

  }

  public void _testMapReduce(boolean restart) throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("hello1\n");
    wr.write("hello2\n");
    wr.write("hello3\n");
    wr.write("hello4\n");
    wr.close();

    if (restart) {
      stopCluster();
      startCluster(false, null);
    }
    
    JobConf conf = createJobConf();
    conf.setJobName("mr");

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(TestClusterMapReduceTestCase.EchoMap.class);
    conf.setReducerClass(TestClusterMapReduceTestCase.EchoReduce.class);

    conf.setInputPath(getInputDir());

    conf.setOutputPath(getOutputDir());


    JobClient.runJob(conf);

    Path[] outputFiles = getFileSystem().listPaths(getOutputDir());

    if (outputFiles.length > 0) {
      InputStream is = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      int counter = 0;
      while (line != null) {
        counter++;
        assertTrue(line.contains("hello"));
        line = reader.readLine();
      }
      reader.close();
      assertEquals(4, counter);
    }

  }

  public void testMapReduce() throws Exception {
    _testMapReduce(false);
  }

  public void testMapReduceRestarting() throws Exception {
    _testMapReduce(true);
  }

  public void testDFSRestart() throws Exception {
    Path file = new Path(getInputDir(), "text.txt");
    OutputStream os = getFileSystem().create(file);
    Writer wr = new OutputStreamWriter(os);
    wr.close();

    stopCluster();
    startCluster(false, null);
    assertTrue(getFileSystem().exists(file));

    stopCluster();
    startCluster(true, null);
    assertFalse(getFileSystem().exists(file));
    
  }

  public void testMRConfig() throws Exception {
    JobConf conf = createJobConf();
    assertNull(conf.get("xyz"));

    Properties config = new Properties();
    config.setProperty("xyz", "XYZ");
    stopCluster();
    startCluster(false, config);

    conf = createJobConf();
    assertEquals("XYZ", conf.get("xyz"));
  }

}
