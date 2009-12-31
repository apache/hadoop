/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Example table column indexing class.  Runs a mapreduce job to index
 * specified table columns.
 * <ul><li>Each row is modeled as a Lucene document: row key is indexed in
 * its untokenized form, column name-value pairs are Lucene field name-value 
 * pairs.</li>
 * <li>A file passed on command line is used to populate an
 * {@link IndexConfiguration} which is used to set various Lucene parameters,
 * specify whether to optimize an index and which columns to index and/or
 * store, in tokenized or untokenized form, etc. For an example, see the
 * <code>createIndexConfContent</code> method in TestTableIndex
 * </li>
 * <li>The number of reduce tasks decides the number of indexes (partitions).
 * The index(es) is stored in the output path of job configuration.</li>
 * <li>The index build process is done in the reduce phase. Users can use
 * the map phase to join rows from different tables or to pre-parse/analyze
 * column content, etc.</li>
 * </ul>
 */
public class BuildTableIndex {

  private static final String USAGE = "Usage: BuildTableIndex " +
    "-r <numReduceTasks> -indexConf <iconfFile>\n" +
    "-indexDir <indexDir> -table <tableName>\n -columns <columnName1> " +
    "[<columnName2> ...]";

  /**
   * Prints the usage message and exists the program.
   * 
   * @param message  The message to print first.
   */
  private static void printUsage(String message) {
    System.err.println(message);
    System.err.println(USAGE);
    System.exit(-1);
  }

  /**
   * Creates a new job.
   * @param conf 
   * 
   * @param args  The command line arguments.
   * @throws IOException When reading the configuration fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args) 
  throws IOException {
    if (args.length < 6) {
      printUsage("Too few arguments");
    }

    int numReduceTasks = 1;
    String iconfFile = null;
    String indexDir = null;
    String tableName = null;
    StringBuilder columnNames = null;

    // parse args
    for (int i = 0; i < args.length - 1; i++) {
      if ("-r".equals(args[i])) {
        numReduceTasks = Integer.parseInt(args[++i]);
      } else if ("-indexConf".equals(args[i])) {
        iconfFile = args[++i];
      } else if ("-indexDir".equals(args[i])) {
        indexDir = args[++i];
      } else if ("-table".equals(args[i])) {
        tableName = args[++i];
      } else if ("-columns".equals(args[i])) {
        columnNames = new StringBuilder(args[++i]);
        while (i + 1 < args.length && !args[i + 1].startsWith("-")) {
          columnNames.append(" ");
          columnNames.append(args[++i]);
        }
      } else {
        printUsage("Unsupported option " + args[i]);
      }
    }

    if (indexDir == null || tableName == null || columnNames == null) {
      printUsage("Index directory, table name and at least one column must " +
        "be specified");
    }

    if (iconfFile != null) {
      // set index configuration content from a file
      String content = readContent(iconfFile);
      IndexConfiguration iconf = new IndexConfiguration();
      // purely to validate, exception will be thrown if not valid
      iconf.addFromXML(content);
      conf.set("hbase.index.conf", content);
    }

    Job job = new Job(conf, "build index for table " + tableName);
    // number of indexes to partition into
    job.setNumReduceTasks(numReduceTasks);
    Scan scan = new Scan();
    for(String columnName : columnNames.toString().split(" ")) {
      String [] fields = columnName.split(":");
      if(fields.length == 1) {
        scan.addFamily(Bytes.toBytes(fields[0]));
      } else {
        scan.addColumn(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1]));
      }
    }
    // use identity map (a waste, but just as an example)
    IdentityTableMapper.initJob(tableName, scan, 
      IdentityTableMapper.class, job);
    // use IndexTableReduce to build a Lucene index
    job.setReducerClass(IndexTableReducer.class);
    FileOutputFormat.setOutputPath(job, new Path(indexDir));
    job.setOutputFormatClass(IndexOutputFormat.class);
    return job;
  }

  /**
   * Reads xml file of indexing configurations.  The xml format is similar to
   * hbase-default.xml and hadoop-default.xml. For an example configuration,
   * see the <code>createIndexConfContent</code> method in TestTableIndex.
   * 
   * @param fileName  The file to read.
   * @return XML configuration read from file.
   * @throws IOException When the XML is broken.
   */
  private static String readContent(String fileName) throws IOException {
    File file = new File(fileName);
    int length = (int) file.length();
    if (length == 0) {
      printUsage("Index configuration file " + fileName + " does not exist");
    }

    int bytesRead = 0;
    byte[] bytes = new byte[length];
    FileInputStream fis = new FileInputStream(file);

    try {
      // read entire file into content
      while (bytesRead < length) {
        int read = fis.read(bytes, bytesRead, length - bytesRead);
        if (read > 0) {
          bytesRead += read;
        } else {
          break;
        }
      }
    } finally {
      fis.close();
    }

    return new String(bytes, 0, bytesRead, HConstants.UTF8_ENCODING);
  }

  /**
   * The main entry point.
   * 
   * @param args  The command line arguments.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = 
      new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
}