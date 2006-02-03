/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io.

import java.io.*;
import java.util.*;
import junit.framework.TestCase;
import java.util.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

/** Support for flat files of binary key/value pairs. */
public class TestArrayFile extends TestCase {
  private static Logger LOG = SequenceFile.LOG;
  private static String FILE =
    System.getProperty("test.build.data",".") + "/test.array";

  public TestArrayFile(String name) { 
      super(name); 
  }

  public void testArrayFile() throws Exception {
      Configuration conf = new Configuration();
    NutchFileSystem nfs = new LocalFileSystem(conf);
    RandomDatum[] data = generate(10000);
    writeTest(nfs, data, FILE);
    readTest(nfs, data, FILE, conf);
  }

  public void testEmptyFile() throws Exception {
    Configuration conf = new Configuration();
    NutchFileSystem nfs = new LocalFileSystem(conf);
    writeTest(nfs, new RandomDatum[0], FILE);
    ArrayFile.Reader reader = new ArrayFile.Reader(nfs, FILE, conf);
    assertNull(reader.get(0, new RandomDatum()));
    reader.close();
  }

  private static RandomDatum[] generate(int count) {
    LOG.fine("generating " + count + " records in memory");
    RandomDatum[] data = new RandomDatum[count];
    RandomDatum.Generator generator = new RandomDatum.Generator();
    for (int i = 0; i < count; i++) {
      generator.next();
      data[i] = generator.getValue();
    }
    return data;
  }

  private static void writeTest(NutchFileSystem nfs, RandomDatum[] data, String file)
    throws IOException {
    MapFile.delete(nfs, file);
    LOG.fine("creating with " + data.length + " records");
    ArrayFile.Writer writer = new ArrayFile.Writer(nfs, file, RandomDatum.class);
    writer.setIndexInterval(100);
    for (int i = 0; i < data.length; i++)
      writer.append(data[i]);
    writer.close();
  }

  private static void readTest(NutchFileSystem nfs, RandomDatum[] data, String file, Configuration conf)
    throws IOException {
    RandomDatum v = new RandomDatum();
    LOG.fine("reading " + data.length + " records");
    ArrayFile.Reader reader = new ArrayFile.Reader(nfs, file, conf);
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
    reader.close();
    LOG.fine("done reading " + data.length + " records");
  }


  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 1024 * 1024;
    boolean create = true;
    boolean check = true;
    String file = FILE;
    String usage = "Usage: TestArrayFile (-local | -dfs <namenode:port>) [-count N] [-nocreate] [-nocheck] file";
      
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    int i = 0;
    NutchFileSystem nfs = NutchFileSystem.parseArgs(args, i, conf);
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
            }
        }

        LOG.info("count = " + count);
        LOG.info("create = " + create);
        LOG.info("check = " + check);
        LOG.info("file = " + file);

        LOG.setLevel(Level.FINE);

        RandomDatum[] data = generate(count);

        if (create) {
            writeTest(nfs, data, file);
        }

        if (check) {
            readTest(nfs, data, file, conf);
        }
    } finally {
        nfs.close();
    }
  }
}
