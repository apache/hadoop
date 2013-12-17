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
package org.apache.hadoop.mapreduce.checkpoint;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

public class TestFSCheckpointID {

  @Test
  public void testFSCheckpointIDSerialization() throws IOException {

    Path inpath = new Path("/tmp/blah");
    FSCheckpointID cidin = new FSCheckpointID(inpath);
    DataOutputBuffer out = new DataOutputBuffer();
    cidin.write(out);
    out.close();

    FSCheckpointID cidout = new FSCheckpointID(null);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), 0, out.getLength());
    cidout.readFields(in);
    in.close();

    assert cidin.equals(cidout);

  }

}
