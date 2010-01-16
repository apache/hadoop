/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.io.File;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;

/**
 * Create a local index, unwrap Lucene documents created by reduce, add them to
 * the index, and copy the index to the destination.
 */
@Deprecated
public class IndexOutputFormat extends
    FileOutputFormat<ImmutableBytesWritable, LuceneDocumentWrapper> {
  static final Log LOG = LogFactory.getLog(IndexOutputFormat.class);

  private Random random = new Random();

  @Override
  public RecordWriter<ImmutableBytesWritable, LuceneDocumentWrapper>
  getRecordWriter(final FileSystem fs, JobConf job, String name,
      final Progressable progress)
  throws IOException {

    final Path perm = new Path(FileOutputFormat.getOutputPath(job), name);
    final Path temp = job.getLocalPath("index/_"
        + Integer.toString(random.nextInt()));

    LOG.info("To index into " + perm);

    // delete old, if any
    fs.delete(perm, true);

    final IndexConfiguration indexConf = new IndexConfiguration();
    String content = job.get("hbase.index.conf");
    if (content != null) {
      indexConf.addFromXML(content);
    }

    String analyzerName = indexConf.getAnalyzerName();
    Analyzer analyzer;
    try {
      Class<?> analyzerClass = Class.forName(analyzerName);
      analyzer = (Analyzer) analyzerClass.newInstance();
    } catch (Exception e) {
      throw new IOException("Error in creating an analyzer object "
          + analyzerName);
    }

    // build locally first
    final IndexWriter writer = new IndexWriter(FSDirectory.open(new File(fs.startLocalOutput(perm, temp)
        .toString())), analyzer, true, IndexWriter.MaxFieldLength.LIMITED);

    // no delete, so no need for maxBufferedDeleteTerms
    writer.setMaxBufferedDocs(indexConf.getMaxBufferedDocs());
    writer.setMaxFieldLength(indexConf.getMaxFieldLength());
    writer.setMaxMergeDocs(indexConf.getMaxMergeDocs());
    writer.setMergeFactor(indexConf.getMergeFactor());
    String similarityName = indexConf.getSimilarityName();
    if (similarityName != null) {
      try {
        Class<?> similarityClass = Class.forName(similarityName);
        Similarity similarity = (Similarity) similarityClass.newInstance();
        writer.setSimilarity(similarity);
      } catch (Exception e) {
        throw new IOException("Error in creating a similarty object "
            + similarityName);
      }
    }
    writer.setUseCompoundFile(indexConf.isUseCompoundFile());

    return new RecordWriter<ImmutableBytesWritable, LuceneDocumentWrapper>() {
      boolean closed;
      private long docCount = 0;

      public void write(ImmutableBytesWritable key,
          LuceneDocumentWrapper value)
      throws IOException {
        // unwrap and index doc
        Document doc = value.get();
        writer.addDocument(doc);
        docCount++;
        progress.progress();
      }

      public void close(final Reporter reporter) throws IOException {
        // spawn a thread to give progress heartbeats
        Thread prog = new Thread() {
          @Override
          public void run() {
            while (!closed) {
              try {
                reporter.setStatus("closing");
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                continue;
              } catch (Throwable e) {
                return;
              }
            }
          }
        };

        try {
          prog.start();

          // optimize index
          if (indexConf.doOptimize()) {
            if (LOG.isInfoEnabled()) {
              LOG.info("Optimizing index.");
            }
            writer.optimize();
          }

          // close index
          writer.close();
          if (LOG.isInfoEnabled()) {
            LOG.info("Done indexing " + docCount + " docs.");
          }

          // copy to perm destination in dfs
          fs.completeLocalOutput(perm, temp);
          if (LOG.isInfoEnabled()) {
            LOG.info("Copy done.");
          }
        } finally {
          closed = true;
        }
      }
    };
  }
}