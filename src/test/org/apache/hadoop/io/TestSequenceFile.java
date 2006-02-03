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

package org.apache.hadoop.io;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;
import java.util.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;


/** Support for flat files of binary key/value pairs. */
public class TestSequenceFile extends TestCase {
  private static Logger LOG = SequenceFile.LOG;

  private static Configuration conf = new Configuration();
  
  public TestSequenceFile(String name) { super(name); }

  /** Unit tests for SequenceFile. */
  public void testSequenceFile() throws Exception {
    int count = 1024 * 10;
    int megabytes = 1;
    int factor = 5;
    String file = System.getProperty("test.build.data",".") + "/test.seq";
 
    int seed = new Random().nextInt();

    FileSystem fs = new LocalFileSystem(new Configuration());
    try {
        //LOG.setLevel(Level.FINE);
        writeTest(fs, count, seed, file, false);
        readTest(fs, count, seed, file);

        sortTest(fs, count, megabytes, factor, false, file);
        checkSort(fs, count, seed, file);

        sortTest(fs, count, megabytes, factor, true, file);
        checkSort(fs, count, seed, file);

        mergeTest(fs, count, seed, file, false, factor, megabytes);
        checkSort(fs, count, seed, file);

        mergeTest(fs, count, seed, file, true, factor, megabytes);
        checkSort(fs, count, seed, file);
    } finally {
        fs.close();
    }
  }

  private static void writeTest(FileSystem fs, int count, int seed,
                                String file, boolean compress)
    throws IOException {
    new File(file).delete();
    LOG.fine("creating with " + count + " records");
    SequenceFile.Writer writer =
      new SequenceFile.Writer(fs, file, RandomDatum.class, RandomDatum.class,
                              compress);
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      writer.append(key, value);
    }
    writer.close();
  }

  private static void readTest(FileSystem fs, int count, int seed, String file)
    throws IOException {
    RandomDatum k = new RandomDatum();
    RandomDatum v = new RandomDatum();
    LOG.fine("reading " + count + " records");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      
      reader.next(k, v);
      
      if (!k.equals(key))
        throw new RuntimeException("wrong key at " + i);
      if (!v.equals(value))
        throw new RuntimeException("wrong value at " + i);
    }
    reader.close();
  }


  private static void sortTest(FileSystem fs, int count, int megabytes, 
                               int factor, boolean fast, String file)
    throws IOException {
    new File(file+".sorted").delete();
    SequenceFile.Sorter sorter = newSorter(fs, fast, megabytes, factor);
    LOG.fine("sorting " + count + " records");
    sorter.sort(file, file+".sorted");
    LOG.fine("done sorting " + count + " records");
  }

  private static void checkSort(FileSystem fs, int count, int seed, String file)
    throws IOException {
    LOG.fine("sorting " + count + " records in memory for check");
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    SortedMap map = new TreeMap();
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      map.put(key, value);
    }

    LOG.fine("checking order of " + count + " records");
    RandomDatum k = new RandomDatum();
    RandomDatum v = new RandomDatum();
    Iterator iterator = map.entrySet().iterator();
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file + ".sorted", conf);
    for (int i = 0; i < count; i++) {
      Map.Entry entry = (Map.Entry)iterator.next();
      RandomDatum key = (RandomDatum)entry.getKey();
      RandomDatum value = (RandomDatum)entry.getValue();

      reader.next(k, v);

      if (!k.equals(key))
        throw new RuntimeException("wrong key at " + i);
      if (!v.equals(value))
        throw new RuntimeException("wrong value at " + i);
    }

    reader.close();
    LOG.fine("sucessfully checked " + count + " records");
  }

  private static void mergeTest(FileSystem fs, int count, int seed, 
                                String file, boolean fast, int factor, 
                                int megabytes)
    throws IOException {

    LOG.fine("creating "+factor+" files with "+count/factor+" records");

    SequenceFile.Writer[] writers = new SequenceFile.Writer[factor];
    String[] names = new String[factor];
    String[] sortedNames = new String[factor];
    
    for (int i = 0; i < factor; i++) {
      names[i] = file+"."+i;
      sortedNames[i] = names[i] + ".sorted";
      fs.delete(new File(names[i]));
      fs.delete(new File(sortedNames[i]));
      writers[i] =
        new SequenceFile.Writer(fs, names[i], RandomDatum.class,RandomDatum.class);
    }

    RandomDatum.Generator generator = new RandomDatum.Generator(seed);

    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      writers[i%factor].append(key, value);
    }

    for (int i = 0; i < factor; i++)
      writers[i].close();

    for (int i = 0; i < factor; i++) {
      LOG.fine("sorting file " + i + " with " + count/factor + " records");
      newSorter(fs, fast, megabytes, factor).sort(names[i], sortedNames[i]);
    }

    LOG.fine("merging " + factor + " files with " + count/factor + " records");
    fs.delete(new File(file+".sorted"));
    newSorter(fs, fast, megabytes, factor).merge(sortedNames, file+".sorted");
  }

  private static SequenceFile.Sorter newSorter(FileSystem fs, 
                                               boolean fast,
                                               int megabytes, int factor) {
    SequenceFile.Sorter sorter = 
      fast
      ? new SequenceFile.Sorter(fs, new RandomDatum.Comparator(),RandomDatum.class, conf)
      : new SequenceFile.Sorter(fs, RandomDatum.class, RandomDatum.class, conf);
    sorter.setMemory(megabytes * 1024*1024);
    sorter.setFactor(factor);
    return sorter;
  }


  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 1024 * 1024;
    int megabytes = 1;
    int factor = 10;
    boolean create = true;
    boolean check = false;
    boolean fast = false;
    boolean merge = false;
    boolean compress = false;
    String file = null;
    String usage = "Usage: SequenceFile (-local | -dfs <namenode:port>) [-count N] [-megabytes M] [-factor F] [-nocreate] [-check] [-fast] [-merge] [-compress] file";
    
    if (args.length == 0) {
        System.err.println(usage);
        System.exit(-1);
    }
    int i = 0;
    FileSystem fs = FileSystem.parseArgs(args, i, conf);      
    try {
      for (; i < args.length; i++) {       // parse command line
          if (args[i] == null) {
              continue;
          } else if (args[i].equals("-count")) {
              count = Integer.parseInt(args[++i]);
          } else if (args[i].equals("-megabytes")) {
              megabytes = Integer.parseInt(args[++i]);
          } else if (args[i].equals("-factor")) {
              factor = Integer.parseInt(args[++i]);
          } else if (args[i].equals("-nocreate")) {
              create = false;
          } else if (args[i].equals("-check")) {
              check = true;
          } else if (args[i].equals("-fast")) {
              fast = true;
          } else if (args[i].equals("-merge")) {
              merge = true;
          } else if (args[i].equals("-compress")) {
              compress = true;
          } else {
              // file is required parameter
              file = args[i];
          }
        }
        LOG.info("count = " + count);
        LOG.info("megabytes = " + megabytes);
        LOG.info("factor = " + factor);
        LOG.info("create = " + create);
        LOG.info("check = " + check);
        LOG.info("fast = " + fast);
        LOG.info("merge = " + merge);
        LOG.info("compress = " + compress);
        LOG.info("file = " + file);

        int seed = 0;
 
        LOG.setLevel(Level.FINE);

        if (create && !merge) {
            writeTest(fs, count, seed, file, compress);
            readTest(fs, count, seed, file);
        }

        if (merge) {
            mergeTest(fs, count, seed, file, fast, factor, megabytes);
        } else {
            sortTest(fs, count, megabytes, factor, fast, file);
        }
    
        if (check) {
            checkSort(fs, count, seed, file);
        }
      } finally {
          fs.close();
      }
  }
}
