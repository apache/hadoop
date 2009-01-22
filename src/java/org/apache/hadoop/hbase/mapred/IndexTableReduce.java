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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Construct a Lucene document per row, which is consumed by IndexOutputFormat
 * to build a Lucene index
 */
public class IndexTableReduce extends MapReduceBase implements
    Reducer<ImmutableBytesWritable, RowResult, ImmutableBytesWritable, LuceneDocumentWrapper> {
  private static final Log LOG = LogFactory.getLog(IndexTableReduce.class);
  private IndexConfiguration indexConf;

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    indexConf = new IndexConfiguration();
    String content = job.get("hbase.index.conf");
    if (content != null) {
      indexConf.addFromXML(content);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Index conf: " + indexConf);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  public void reduce(ImmutableBytesWritable key, Iterator<RowResult> values,
      OutputCollector<ImmutableBytesWritable, LuceneDocumentWrapper> output,
      Reporter reporter)
  throws IOException {
    if (!values.hasNext()) {
      return;
    }

    Document doc = new Document();

    // index and store row key, row key already UTF-8 encoded
    Field keyField = new Field(indexConf.getRowkeyName(),
      Bytes.toString(key.get()),
      Field.Store.YES, Field.Index.UN_TOKENIZED);
    keyField.setOmitNorms(true);
    doc.add(keyField);

    while (values.hasNext()) {
      RowResult value = values.next();

      // each column (name-value pair) is a field (name-value pair)
      for (Map.Entry<byte [], Cell> entry : value.entrySet()) {
        // name is already UTF-8 encoded
        String column = Bytes.toString(entry.getKey());
        byte[] columnValue = entry.getValue().getValue();
        Field.Store store = indexConf.isStore(column)?
          Field.Store.YES: Field.Store.NO;
        Field.Index index = indexConf.isIndex(column)?
          (indexConf.isTokenize(column)?
            Field.Index.TOKENIZED: Field.Index.UN_TOKENIZED):
            Field.Index.NO;

        // UTF-8 encode value
        Field field = new Field(column, Bytes.toString(columnValue), 
          store, index);
        field.setBoost(indexConf.getBoost(column));
        field.setOmitNorms(indexConf.isOmitNorms(column));

        doc.add(field);
      }
    }
    output.collect(key, new LuceneDocumentWrapper(doc));
  }
}
