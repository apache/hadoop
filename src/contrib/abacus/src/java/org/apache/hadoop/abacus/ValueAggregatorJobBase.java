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

package org.apache.hadoop.abacus;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.JobConf;


/**
 * This abstract class implements some common functionalities of the
 * the generic mapper, reducer and combiner classes of Abacus.
 *
 */
public abstract class ValueAggregatorJobBase extends JobBase {
 
  protected ArrayList aggregatorDescriptorList = null;
        
  public void configure(JobConf job) {
    super.configure(job);
        
    setLongValue("totalCount", 0);
    setLongValue("errorCount", 0);
    setLongValue("collectedCount", 0);
    setLongValue("groupCount", 0);
        
    this.initializeMySpec(job);
    this.logSpec();
  }

  private static ValueAggregatorDescriptor getValueAggregatorDescriptor(
                                                                        String spec, JobConf job) {
    if (spec == null)
      return null;
    String[] segments = spec.split(",", -1);
    String type = segments[0];
    if (type.compareToIgnoreCase("UserDefined") == 0) {
      String className = segments[1];
      return new UserDefinedValueAggregatorDescriptor(className, job);
    } 
    return null;
  }

  private static ArrayList getAggregatorDescriptors(JobConf job) {
    String advn = "aggregator.descriptor";
    int num = job.getInt(advn + ".num", 0);
    ArrayList retv = new ArrayList(num);
    for (int i = 0; i < num; i++) {
      String spec = job.get(advn + "." + i);
      ValueAggregatorDescriptor ad = getValueAggregatorDescriptor(spec, job);
      if (ad != null) {
        retv.add(ad);
      }
    }
    return retv;
  }
    
  private void initializeMySpec(JobConf job) {
    this.aggregatorDescriptorList = getAggregatorDescriptors(job);
    if (this.aggregatorDescriptorList.size() == 0) {
      this.aggregatorDescriptorList.add(new UserDefinedValueAggregatorDescriptor(
                                                                                 ValueAggregatorBaseDescriptor.class.getCanonicalName(), job));
    }
  }
    
  protected void logSpec() {
    StringBuffer sb = new StringBuffer();
    sb.append("\n");
    if (aggregatorDescriptorList == null) {
      sb.append(" aggregatorDescriptorList: null");
    } else {
      sb.append(" aggregatorDescriptorList: ");
      for (int i = 0; i < aggregatorDescriptorList.size(); i++) {
        sb.append(" ").append(aggregatorDescriptorList.get(i).toString());
      }
    }      
    LOG.info(sb.toString());
  }

  public void close() throws IOException {
    report();
  }
}
