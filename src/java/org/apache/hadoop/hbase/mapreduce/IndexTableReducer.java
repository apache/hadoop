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

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Construct a Lucene document per row, which is consumed by IndexOutputFormat
 * to build a Lucene index
 */
public class IndexTableReducer 
extends Reducer<ImmutableBytesWritable, Result, 
    ImmutableBytesWritable, LuceneDocumentWrapper>
implements Configurable {
  
  private static final Log LOG = LogFactory.getLog(IndexTableReducer.class);
  
  private IndexConfiguration indexConf;
  private Configuration conf = null;
  
  /**
   * Writes each given record, consisting of the key and the given values, to
   * the index.
   * 
   * @param key  The current row key.
   * @param values  The values for the given row.
   * @param context  The context of the reduce. 
   * @throws IOException When writing the record fails.
   * @throws InterruptedException When the job gets interrupted.
   */
  @Override
  public void reduce(ImmutableBytesWritable key, Iterable<Result> values,
      Context context)
  throws IOException, InterruptedException {
    Document doc = null;
    for (Result r : values) {
      if (doc == null) {
        doc = new Document();
        // index and store row key, row key already UTF-8 encoded
        Field keyField = new Field(indexConf.getRowkeyName(),
          Bytes.toString(key.get(), key.getOffset(), key.getLength()),
          Field.Store.YES, Field.Index.UN_TOKENIZED);
        keyField.setOmitNorms(true);
        doc.add(keyField);
      }
      // each column (name-value pair) is a field (name-value pair)
      for (KeyValue kv: r.list()) {
        // name is already UTF-8 encoded
        String column = Bytes.toString(kv.getColumn());
        byte[] columnValue = kv.getValue();
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
    context.write(key, new LuceneDocumentWrapper(doc));
  }

  /**
   * Returns the current configuration.
   *  
   * @return The current configuration.
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Sets the configuration. This is used to set up the index configuration.
   * 
   * @param configuration  The configuration to set.
   * @see org.apache.hadoop.conf.Configurable#setConf(
   *   org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    indexConf = new IndexConfiguration();
    String content = conf.get("hbase.index.conf");
    if (content != null) {
      indexConf.addFromXML(content);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Index conf: " + indexConf);
    }
  }
  
}
