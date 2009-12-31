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
import java.io.IOException;
import java.util.Random;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MultiRegionTable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Searchable;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;

/**
 * Test Map/Reduce job to build index over HBase table
 */
public class DisabledBecauseVariableSubstTooLargeExceptionTestTableIndex extends MultiRegionTable {
  private static final Log LOG = LogFactory.getLog(DisabledBecauseVariableSubstTooLargeExceptionTestTableIndex.class);

  static final byte[] TABLE_NAME = Bytes.toBytes("moretest");
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final byte[] OUTPUT_FAMILY = Bytes.toBytes("text");
  static final String ROWKEY_NAME = "key";
  static final String INDEX_DIR = "testindex";

  static final Random rand = new Random();

  /** default constructor */
  public DisabledBecauseVariableSubstTooLargeExceptionTestTableIndex() {
    super(Bytes.toString(INPUT_FAMILY));
    desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(INPUT_FAMILY));
    desc.addFamily(new HColumnDescriptor(OUTPUT_FAMILY));
  }

    @Override
  public void tearDown() throws Exception {
    if (conf != null) {
      FileUtil.fullyDelete(new File(conf.get("hadoop.tmp.dir")));
    }
    super.tearDown();
  }

  /**
   * Test HBase map/reduce
   * 
   * @throws IOException
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   */
  public void testTableIndex() 
  throws IOException, InterruptedException, ClassNotFoundException {
    boolean printResults = false;
    if (printResults) {
      LOG.info("Print table contents before map/reduce");
    }
    scanTable(printResults);

    MiniMRCluster mrCluster = new MiniMRCluster(2, fs.getUri().toString(), 1);

    // set configuration parameter for index build
    conf.set("hbase.index.conf", createIndexConfContent());

    try {
      Job job = new Job(conf, "index column contents");
      // number of indexes to partition into
      job.setNumReduceTasks(1);

      Scan scan = new Scan();
      scan.addFamily(INPUT_FAMILY);
      // use identity map (a waste, but just as an example)
      IdentityTableMapper.initJob(Bytes.toString(TABLE_NAME), scan, 
        IdentityTableMapper.class, job);
      // use IndexTableReduce to build a Lucene index
      job.setReducerClass(IndexTableReducer.class);
      job.setOutputFormatClass(IndexOutputFormat.class);
      FileOutputFormat.setOutputPath(job, new Path(INDEX_DIR));
      job.waitForCompletion(true);
    } finally {
      mrCluster.shutdown();
    }

    if (printResults) {
      LOG.info("Print table contents after map/reduce");
    }
    scanTable(printResults);

    // verify index results
    verify();
  }

  private String createIndexConfContent() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<configuration><column><property>" +
      "<name>hbase.column.name</name><value>" + Bytes.toString(INPUT_FAMILY) +
      "</value></property>");
    buffer.append("<property><name>hbase.column.store</name> " +
      "<value>true</value></property>");
    buffer.append("<property><name>hbase.column.index</name>" +
      "<value>true</value></property>");
    buffer.append("<property><name>hbase.column.tokenize</name>" +
      "<value>false</value></property>");
    buffer.append("<property><name>hbase.column.boost</name>" +
      "<value>3</value></property>");
    buffer.append("<property><name>hbase.column.omit.norms</name>" +
      "<value>false</value></property></column>");
    buffer.append("<property><name>hbase.index.rowkey.name</name><value>" +
      ROWKEY_NAME + "</value></property>");
    buffer.append("<property><name>hbase.index.max.buffered.docs</name>" +
      "<value>500</value></property>");
    buffer.append("<property><name>hbase.index.max.field.length</name>" +
      "<value>10000</value></property>");
    buffer.append("<property><name>hbase.index.merge.factor</name>" +
      "<value>10</value></property>");
    buffer.append("<property><name>hbase.index.use.compound.file</name>" +
      "<value>true</value></property>");
    buffer.append("<property><name>hbase.index.optimize</name>" +
      "<value>true</value></property></configuration>");

    IndexConfiguration c = new IndexConfiguration();
    c.addFromXML(buffer.toString());
    return c.toString();
  }

  private void scanTable(boolean printResults)
  throws IOException {
    HTable table = new HTable(conf, TABLE_NAME);
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    scan.addFamily(OUTPUT_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    try {
      for (Result r : scanner) {
        if (printResults) {
          LOG.info("row: " + Bytes.toStringBinary(r.getRow()));
        }
        for (KeyValue kv : r.list()) {
          if (printResults) {
            LOG.info(" column: " + Bytes.toStringBinary(kv.getKey()) + " value: "
              + Bytes.toStringBinary(kv.getValue()));
          }
        }
      }
    } finally {
      scanner.close();
    }
  }

  private void verify() throws IOException {
    // Force a cache flush for every online region to ensure that when the
    // scanner takes its snapshot, all the updates have made it into the cache.
    for (HRegion r : cluster.getRegionThreads().get(0).getRegionServer().
        getOnlineRegions()) {
      HRegionIncommon region = new HRegionIncommon(r);
      region.flushcache();
    }

    Path localDir = new Path(getUnitTestdir(getName()), "index_" +
      Integer.toString(rand.nextInt()));
    this.fs.copyToLocalFile(new Path(INDEX_DIR), localDir);
    FileSystem localfs = FileSystem.getLocal(conf);
    FileStatus [] indexDirs = localfs.listStatus(localDir);
    Searcher searcher = null;
    ResultScanner scanner = null;
    try {
      if (indexDirs.length == 1) {
        searcher = new IndexSearcher((new File(indexDirs[0].getPath().
          toUri())).getAbsolutePath());
      } else if (indexDirs.length > 1) {
        Searchable[] searchers = new Searchable[indexDirs.length];
        for (int i = 0; i < indexDirs.length; i++) {
          searchers[i] = new IndexSearcher((new File(indexDirs[i].getPath().
            toUri()).getAbsolutePath()));
        }
        searcher = new MultiSearcher(searchers);
      } else {
        throw new IOException("no index directory found");
      }

      HTable table = new HTable(conf, TABLE_NAME);
      Scan scan = new Scan();
      scan.addFamily(INPUT_FAMILY);
      scan.addFamily(OUTPUT_FAMILY);
      scanner = table.getScanner(scan);

      IndexConfiguration indexConf = new IndexConfiguration();
      String content = conf.get("hbase.index.conf");
      if (content != null) {
        indexConf.addFromXML(content);
      }
      String rowkeyName = indexConf.getRowkeyName();

      int count = 0;
      for (Result r : scanner) {
        String value = Bytes.toString(r.getRow());
        Term term = new Term(rowkeyName, value);
        int hitCount = searcher.search(new TermQuery(term)).length();
        assertEquals("check row " + value, 1, hitCount);
        count++;
      }
      LOG.debug("Searcher.maxDoc: " + searcher.maxDoc());
      LOG.debug("IndexReader.numDocs: " + ((IndexSearcher)searcher).getIndexReader().numDocs());      
      int maxDoc = ((IndexSearcher)searcher).getIndexReader().numDocs();
      assertEquals("check number of rows", maxDoc, count);
    } finally {
      if (null != searcher)
        searcher.close();
      if (null != scanner)
        scanner.close();
    }
  }
  /**
   * @param args unused
   */
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(DisabledBecauseVariableSubstTooLargeExceptionTestTableIndex.class));
  }
}