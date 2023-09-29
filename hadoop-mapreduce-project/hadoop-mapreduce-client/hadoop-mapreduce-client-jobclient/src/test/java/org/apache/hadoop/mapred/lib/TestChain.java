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
package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestChain {
  @Test
  public void testSetReducerWithReducerByValueAsTrue() throws Exception {

    JobConf jobConf = new JobConf();
    JobConf reducerConf = new JobConf();
    Chain.setReducer(jobConf, MyReducer.class, Object.class, Object.class,
        Object.class, Object.class, true, reducerConf);
    boolean reduceByValue = reducerConf.getBoolean("chain.reducer.byValue",
        false);
    assertThat(reduceByValue).withFailMessage(
        "It should set chain.reducer.byValue as true in "
            + "reducerConf when we give value as true").isTrue();
  }

  @Test
  public void testSetReducerWithReducerByValueAsFalse() throws Exception {

    JobConf jobConf = new JobConf();
    JobConf reducerConf = new JobConf();
    Chain.setReducer(jobConf, MyReducer.class, Object.class, Object.class,
        Object.class, Object.class, false, reducerConf);
    boolean reduceByValue = reducerConf.getBoolean("chain.reducer.byValue",
        true);
    assertThat(reduceByValue).withFailMessage(
        "It should set chain.reducer.byValue as false "
            + "in reducerConf when we give value as false").isFalse();
  }

  interface MyReducer extends Reducer<Object, Object, Object, Object> {

  }
}
