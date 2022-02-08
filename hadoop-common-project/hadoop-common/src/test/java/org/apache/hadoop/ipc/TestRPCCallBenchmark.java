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
package org.apache.hadoop.ipc;

import static org.junit.Assert.*;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestRPCCallBenchmark {
  @Parameterized.Parameters(name="{index}: useNetty={0}")
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{Boolean.FALSE});
    params.add(new Object[]{Boolean.TRUE});
    return params;
  }

  private static boolean useNetty;
  public TestRPCCallBenchmark(Boolean useNetty) {
    this.useNetty = useNetty;
  }

  @Test(timeout=60000)
  public void testBenchmarkWithProto() throws Exception {
    int rc = ToolRunner.run(new RPCCallBenchmark(),
        new String[] {
      "--clientThreads", "30",
      "--serverThreads", "30",
      "--time", "10",
      "--serverReaderThreads", "4",
      "--messageSize", "1024",
      "--engine", "protobuf",
      "--ioImpl", useNetty ? "netty" : "nio"});
    assertEquals(0, rc);
  }
}
