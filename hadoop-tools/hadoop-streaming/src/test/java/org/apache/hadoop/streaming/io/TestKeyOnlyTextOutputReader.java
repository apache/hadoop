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

package org.apache.hadoop.streaming.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.PipeMapper;
import org.junit.Test;

public class TestKeyOnlyTextOutputReader {
  @Test
  public void testKeyOnlyTextOutputReader() throws IOException {
    String text = "key,value\nkey2,value2\nnocomma\n";
    PipeMapRed pipeMapRed = new MyPipeMapRed(text);
    KeyOnlyTextOutputReader outputReader = new KeyOnlyTextOutputReader();
    outputReader.initialize(pipeMapRed);
    outputReader.readKeyValue();
    Assert.assertEquals(new Text("key,value"), outputReader.getCurrentKey());
    outputReader.readKeyValue();
    Assert.assertEquals(new Text("key2,value2"), outputReader.getCurrentKey());
    outputReader.readKeyValue();
    Assert.assertEquals(new Text("nocomma"), outputReader.getCurrentKey());
    Assert.assertEquals(false, outputReader.readKeyValue());
  }
  
  private class MyPipeMapRed extends PipeMapper {
    private DataInput clientIn;
    private Configuration conf = new Configuration();
    
    public MyPipeMapRed(String text) {
      clientIn = new DataInputStream(new ByteArrayInputStream(text.getBytes()));
    }
    
    @Override
    public DataInput getClientInput() {
      return clientIn;
    }
    
    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }
}
