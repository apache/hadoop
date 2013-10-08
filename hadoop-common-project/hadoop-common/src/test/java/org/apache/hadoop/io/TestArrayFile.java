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

package org.apache.hadoop.io;

import java.io.*;

import junit.framework.TestCase;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.conf.*;

/** Support for flat files of binary key/value pairs. */
public class TestArrayFile extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestArrayFile.class);
  
  private static final Path TEST_DIR = new Path(
      System.getProperty("test.build.data", "/tmp"),
      TestMapFile.class.getSimpleName());
  private static String TEST_FILE = new Path(TEST_DIR, "test.array").toString();

  public TestArrayFile(String name) { 
    super(name); 
  }

  public void testArrayFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    RandomDatum[] data = generate(10000);
    writeTest(fs, data, TEST_FILE);
    readTest(fs, data, TEST_FILE, conf);
  }

  public void testEmptyFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    writeTest(fs, new RandomDatum[0], TEST_FILE);
    ArrayFile.Reader reader = new ArrayFile.Reader(fs, TEST_FILE, conf);
    assertNull(reader.get(0, new RandomDatum()));
    reader.close();
  }

  private static RandomDatum[] generate(int count) {
    if(LOG.isDebugEnabled()) {
      LOG.debug("generating " + count + " records in debug");
    }
    RandomDatum[] data = new RandomDatum[count];
    RandomDatum.Generator generator = new RandomDatum.Generator();
    for (int i = 0; i < count; i++) {
      generator.next();
      data[i] = generator.getValue();
    }
    return data;
  }

  private static void writeTest(FileSystem fs, RandomDatum[] data, String file)
    throws IOException {
    Configuration conf = new Configuration();
    MapFile.delete(fs, file);
    if(LOG.isDebugEnabled()) {
      LOG.debug("creating with " + data.length + " debug");
    }
    ArrayFile.Writer writer = new ArrayFile.Writer(conf, fs, file, RandomDatum.class);
    writer.setIndexInterval(100);
    for (int i = 0; i < data.length; i++)
      writer.append(data[i]);
    writer.close();
  }

  private static void readTest(FileSystem fs, RandomDatum[] data, String file, Configuration conf)
    throws IOException {
    RandomDatum v = new RandomDatum();
    if(LOG.isDebugEnabled()) {
      LOG.debug("reading " + data.length + " debug");
    }
    ArrayFile.Reader reader = new ArrayFile.Reader(fs, file, conf);
    try {
      for (int i = 0; i < data.length; i++) {       // try forwards
        reader.get(i, v);
        if (!v.equals(data[i])) {
          throw new RuntimeException("wrong value at " + i);
        }
      }
      for (int i = data.length-1; i >= 0; i--) {    // then backwards
        reader.get(i, v);
        if (!v.equals(data[i])) {
          throw new RuntimeException("wrong value at " + i);
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("done reading " + data.length + " debug");
      }
    } finally {
      reader.close();
    }
  }

  /** 
   * test on {@link ArrayFile.Reader} iteration methods
   * <pre> 
   * {@code next(), seek()} in and out of range.
   * </pre>
   */
  public void testArrayFileIteration() {
    int SIZE = 10;
    Configuration conf = new Configuration();    
    try {
      FileSystem fs = FileSystem.get(conf);
      ArrayFile.Writer writer = new ArrayFile.Writer(conf, fs, TEST_FILE, 
          LongWritable.class, CompressionType.RECORD, defaultProgressable);
      assertNotNull("testArrayFileIteration error !!!", writer);
      
      for (int i = 0; i < SIZE; i++)
        writer.append(new LongWritable(i));
      
      writer.close();
      
      ArrayFile.Reader reader = new ArrayFile.Reader(fs, TEST_FILE, conf);
      LongWritable nextWritable = new LongWritable(0);
      
      for (int i = 0; i < SIZE; i++) {
        nextWritable = (LongWritable)reader.next(nextWritable);
        assertEquals(nextWritable.get(), i);
      }
        
      assertTrue("testArrayFileIteration seek error !!!",
          reader.seek(new LongWritable(6)));
      nextWritable = (LongWritable) reader.next(nextWritable);
      assertTrue("testArrayFileIteration error !!!", reader.key() == 7);
      assertTrue("testArrayFileIteration error !!!",
          nextWritable.equals(new LongWritable(7)));
      assertFalse("testArrayFileIteration error !!!",
          reader.seek(new LongWritable(SIZE + 5)));
      reader.close();
    } catch (Exception ex) {
      fail("testArrayFileWriterConstruction error !!!");
    }
  }
 
  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 1024 * 1024;
    boolean create = true;
    boolean check = true;
    String file = TEST_FILE;
    String usage = "Usage: TestArrayFile [-count N] [-nocreate] [-nocheck] file";
      
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    int i = 0;
    Path fpath = null;
    FileSystem fs = null;
    try {
      for (; i < args.length; i++) {       // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-nocreate")) {
          create = false;
        } else if (args[i].equals("-nocheck")) {
          check = false;
        } else {                                       
          // file is required parameter
          file = args[i];
          fpath=new Path(file);
        }
      }
        
      fs = fpath.getFileSystem(conf);
        
      LOG.info("count = " + count);
      LOG.info("create = " + create);
      LOG.info("check = " + check);
      LOG.info("file = " + file);

      RandomDatum[] data = generate(count);

      if (create) {
        writeTest(fs, data, file);
      }

      if (check) {
        readTest(fs, data, file, conf);
      }
    } finally {
      fs.close();
    }
  }
  
  private static final Progressable defaultProgressable = new Progressable() {
    @Override
    public void progress() {      
    }
  };
  
}
