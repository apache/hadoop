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
package org.apache.hadoop.mapreduce.lib.chain;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TestMapReduceChain extends HadoopTestCase {

  private static String localPathRoot = System.getProperty("test.build.data",
      "/tmp");
  private static Path flagDir = new Path(localPathRoot, "testing/chain/flags");

  private static void cleanFlags(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.delete(flagDir, true);
    fs.mkdirs(flagDir);
  }

  private static void writeFlag(Configuration conf, String flag)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (getFlag(conf, flag)) {
      fail("Flag " + flag + " already exists");
    }
    DataOutputStream file = fs.create(new Path(flagDir, flag));
    file.close();
  }

  private static boolean getFlag(Configuration conf, String flag)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    return fs.exists(new Path(flagDir, flag));
  }

  public TestMapReduceChain() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  public void testChain() throws Exception {
    Path inDir = new Path(localPathRoot, "testing/chain/input");
    Path outDir = new Path(localPathRoot, "testing/chain/output");
    String input = "1\n2\n";
    String expectedOutput = "0\t1ABCRDEF\n2\t2ABCRDEF\n";

    Configuration conf = createJobConf();
    cleanFlags(conf);
    conf.set("a", "X");

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 1, input);
    job.setJobName("chain");

    Configuration mapAConf = new Configuration(false);
    mapAConf.set("a", "A");
    ChainMapper.addMapper(job, AMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, mapAConf);

    ChainMapper.addMapper(job, BMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    ChainMapper.addMapper(job, CMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    Configuration reduceConf = new Configuration(false);
    reduceConf.set("a", "C");
    ChainReducer.setReducer(job, RReduce.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, reduceConf);

    ChainReducer.addMapper(job, DMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    Configuration mapEConf = new Configuration(false);
    mapEConf.set("a", "E");
    ChainReducer.addMapper(job, EMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, mapEConf);

    ChainReducer.addMapper(job, FMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());

    String str = "flag not set";
    assertTrue(str, getFlag(conf, "map.setup.A"));
    assertTrue(str, getFlag(conf, "map.setup.B"));
    assertTrue(str, getFlag(conf, "map.setup.C"));
    assertTrue(str, getFlag(conf, "reduce.setup.R"));
    assertTrue(str, getFlag(conf, "map.setup.D"));
    assertTrue(str, getFlag(conf, "map.setup.E"));
    assertTrue(str, getFlag(conf, "map.setup.F"));

    assertTrue(str, getFlag(conf, "map.A.value.1"));
    assertTrue(str, getFlag(conf, "map.A.value.2"));
    assertTrue(str, getFlag(conf, "map.B.value.1A"));
    assertTrue(str, getFlag(conf, "map.B.value.2A"));
    assertTrue(str, getFlag(conf, "map.C.value.1AB"));
    assertTrue(str, getFlag(conf, "map.C.value.2AB"));
    assertTrue(str, getFlag(conf, "reduce.R.value.1ABC"));
    assertTrue(str, getFlag(conf, "reduce.R.value.2ABC"));
    assertTrue(str, getFlag(conf, "map.D.value.1ABCR"));
    assertTrue(str, getFlag(conf, "map.D.value.2ABCR"));
    assertTrue(str, getFlag(conf, "map.E.value.1ABCRD"));
    assertTrue(str, getFlag(conf, "map.E.value.2ABCRD"));
    assertTrue(str, getFlag(conf, "map.F.value.1ABCRDE"));
    assertTrue(str, getFlag(conf, "map.F.value.2ABCRDE"));

    assertTrue(getFlag(conf, "map.cleanup.A"));
    assertTrue(getFlag(conf, "map.cleanup.B"));
    assertTrue(getFlag(conf, "map.cleanup.C"));
    assertTrue(getFlag(conf, "reduce.cleanup.R"));
    assertTrue(getFlag(conf, "map.cleanup.D"));
    assertTrue(getFlag(conf, "map.cleanup.E"));
    assertTrue(getFlag(conf, "map.cleanup.F"));

    assertEquals("Outputs doesn't match", expectedOutput, MapReduceTestUtil
        .readOutput(outDir, conf));
  }

  public static class AMap extends IDMap {
    public AMap() {
      super("A", "A");
    }
  }

  public static class BMap extends IDMap {
    public BMap() {
      super("B", "X");
    }
  }

  public static class CMap extends IDMap {
    public CMap() {
      super("C", "X");
    }
  }

  public static class RReduce extends IDReduce {
    public RReduce() {
      super("R", "C");
    }
  }

  public static class DMap extends IDMap {
    public DMap() {
      super("D", "X");
    }
  }

  public static class EMap extends IDMap {
    public EMap() {
      super("E", "E");
    }
  }

  public static class FMap extends IDMap {
    public FMap() {
      super("F", "X");
    }
  }

  public static class IDMap extends
      Mapper<LongWritable, Text, LongWritable, Text> {
    private String name;
    private String prop;

    public IDMap(String name, String prop) {
      this.name = name;
      this.prop = prop;
    }

    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      assertEquals(prop, conf.get("a"));
      writeFlag(conf, "map.setup." + name);
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      writeFlag(context.getConfiguration(), "map." + name + ".value." + value);
      context.write(key, new Text(value + name));
    }

    public void cleanup(Context context) throws IOException,
        InterruptedException {
      writeFlag(context.getConfiguration(), "map.cleanup." + name);
    }
  }

  public static class IDReduce extends
      Reducer<LongWritable, Text, LongWritable, Text> {

    private String name;
    private String prop;

    public IDReduce(String name, String prop) {
      this.name = name;
      this.prop = prop;
    }

    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      assertEquals(prop, conf.get("a"));
      writeFlag(conf, "reduce.setup." + name);
    }

    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        writeFlag(context.getConfiguration(), "reduce." + name + ".value."
            + value);
        context.write(key, new Text(value + name));
      }
    }

    public void cleanup(Context context) throws IOException,
        InterruptedException {
      writeFlag(context.getConfiguration(), "reduce.cleanup." + name);
    }
  }
}
