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
import org.apache.hadoop.util.LogFormatter;

/** Support for flat files of binary key/value pairs. */
public class TestSetFile extends TestCase {
  private static Logger LOG = SequenceFile.LOG;
  private static String FILE =
    System.getProperty("test.build.data",".") + "/test.set";

  private static Configuration conf = new Configuration();
  
  public TestSetFile(String name) { super(name); }

  public void testSetFile() throws Exception {
    NutchFileSystem nfs = new LocalFileSystem(conf);
    try {
        RandomDatum[] data = generate(10000);
        writeTest(nfs, data, FILE);
        readTest(nfs, data, FILE);
    } finally {
        nfs.close();
    }
  }

  private static RandomDatum[] generate(int count) {
    LOG.fine("generating " + count + " records in memory");
    RandomDatum[] data = new RandomDatum[count];
    RandomDatum.Generator generator = new RandomDatum.Generator();
    for (int i = 0; i < count; i++) {
      generator.next();
      data[i] = generator.getValue();
    }
    LOG.fine("sorting " + count + " records in memory");
    Arrays.sort(data);
    return data;
  }

  private static void writeTest(NutchFileSystem nfs, RandomDatum[] data, String file)
    throws IOException {
    MapFile.delete(nfs, file);
    LOG.fine("creating with " + data.length + " records");
    SetFile.Writer writer = new SetFile.Writer(nfs, file, RandomDatum.class);
    for (int i = 0; i < data.length; i++)
      writer.append(data[i]);
    writer.close();
  }

  private static void readTest(NutchFileSystem nfs, RandomDatum[] data, String file)
    throws IOException {
    RandomDatum v = new RandomDatum();
    LOG.fine("reading " + data.length + " records");
    SetFile.Reader reader = new SetFile.Reader(nfs, file, conf);
    for (int i = 0; i < data.length; i++) {
      if (!reader.seek(data[i]))
        throw new RuntimeException("wrong value at " + i);
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
    String usage = "Usage: TestSetFile (-local | -dfs <namenode:port>) [-count N] [-nocreate] [-nocheck] file";
      
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      
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
          readTest(nfs, data, file);
        }
      }
    } finally {
      nfs.close();
    }
  }
}
