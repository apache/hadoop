/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Progressable;

public class TestJavaSerialization extends ClusterMapReduceTestCase {
  
  static class TypeConverterMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, Long, String> {

    public void map(LongWritable key, Text value,
        OutputCollector<Long, String> output, Reporter reporter)
        throws IOException {
      output.collect(key.get(), value.toString());
    }

  }
  
  static class StringOutputFormat<K, V> extends OutputFormatBase<K, V> {
    
    static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
      
      private DataOutputStream out;
      
      public LineRecordWriter(DataOutputStream out) {
        this.out = out;
      }

      public void close(Reporter reporter) throws IOException {
        out.close();
      }

      public void write(K key, V value) throws IOException {
        print(key);
        print("\t");
        print(value);
        print("\n");
      }
      
      private void print(Object o) throws IOException {
        out.write(o.toString().getBytes("UTF-8"));
      }
      
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
        String name, Progressable progress) throws IOException {

      Path dir = job.getOutputPath();
      FileSystem fs = dir.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
      return new LineRecordWriter<K, V>(fileOut);
    }
    
  }
  
  public void testMapReduceJob() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("hello1\n");
    wr.write("hello2\n");
    wr.write("hello3\n");
    wr.write("hello4\n");
    wr.close();

    JobConf conf = createJobConf();
    conf.setJobName("JavaSerialization");
    
    conf.set("io.serializations",
    "org.apache.hadoop.io.serializer.JavaSerialization," +
    "org.apache.hadoop.io.serializer.WritableSerialization");

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(Long.class);
    conf.setMapOutputValueClass(String.class);

    conf.setOutputFormat(StringOutputFormat.class);
    conf.setOutputKeyClass(Long.class);
    conf.setOutputValueClass(String.class);
    conf.setOutputKeyComparatorClass(JavaSerializationComparator.class);

    conf.setMapperClass(TypeConverterMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    conf.setInputPath(getInputDir());

    conf.setOutputPath(getOutputDir());

    JobClient.runJob(conf);

    Path[] outputFiles = FileUtil.stat2Paths(
                           getFileSystem().listStatus(getOutputDir(),
                           new OutputLogFilter()));
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

}
