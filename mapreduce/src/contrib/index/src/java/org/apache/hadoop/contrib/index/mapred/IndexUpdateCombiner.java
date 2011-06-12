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

package org.apache.hadoop.contrib.index.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This combiner combines multiple intermediate forms into one intermediate
 * form. More specifically, the input intermediate forms are a single-document
 * ram index and/or a single delete term. An output intermediate form contains
 * a multi-document ram index and/or multiple delete terms.   
 */
public class IndexUpdateCombiner extends MapReduceBase implements
    Reducer<Shard, IntermediateForm, Shard, IntermediateForm> {
  static final Log LOG = LogFactory.getLog(IndexUpdateCombiner.class);

  IndexUpdateConfiguration iconf;
  long maxSizeInBytes;
  long nearMaxSizeInBytes;

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  public void reduce(Shard key, Iterator<IntermediateForm> values,
      OutputCollector<Shard, IntermediateForm> output, Reporter reporter)
      throws IOException {

    String message = key.toString();
    IntermediateForm form = null;

    while (values.hasNext()) {
      IntermediateForm singleDocForm = values.next();
      long formSize = form == null ? 0 : form.totalSizeInBytes();
      long singleDocFormSize = singleDocForm.totalSizeInBytes();

      if (form != null && formSize + singleDocFormSize > maxSizeInBytes) {
        closeForm(form, message);
        output.collect(key, form);
        form = null;
      }

      if (form == null && singleDocFormSize >= nearMaxSizeInBytes) {
        output.collect(key, singleDocForm);
      } else {
        if (form == null) {
          form = createForm(message);
        }
        form.process(singleDocForm);
      }
    }

    if (form != null) {
      closeForm(form, message);
      output.collect(key, form);
    }
  }

  private IntermediateForm createForm(String message) throws IOException {
    LOG.info("Construct a form writer for " + message);
    IntermediateForm form = new IntermediateForm();
    form.configure(iconf);
    return form;
  }

  private void closeForm(IntermediateForm form, String message)
      throws IOException {
    form.closeWriter();
    LOG.info("Closed the form writer for " + message + ", form = " + form);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    iconf = new IndexUpdateConfiguration(job);
    maxSizeInBytes = iconf.getMaxRAMSizeInBytes();
    nearMaxSizeInBytes = maxSizeInBytes - (maxSizeInBytes >>> 3); // 7/8 of max
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.MapReduceBase#close()
   */
  public void close() throws IOException {
  }

}
