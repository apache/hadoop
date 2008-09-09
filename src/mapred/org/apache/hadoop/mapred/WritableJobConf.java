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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

/**
 * Writable JobConf is the Writable version of the JobConf. 
 */
public class WritableJobConf extends JobConf implements Writable {

  /**
   * This C.tor does not load default configuration files, 
   * hadoop-(default|site).xml
   */
  public WritableJobConf() {
    super(false); //do not load defaults
  }

  public WritableJobConf(boolean loadDefaults) {
    super(loadDefaults);    
  }

  public WritableJobConf(Class<?> exampleClass) {
    super(exampleClass);
  }

  public WritableJobConf(Configuration conf, Class<?> exampleClass) {
    super(conf, exampleClass);
  }

  public WritableJobConf(Configuration conf) {
    super(conf);
  }

  public WritableJobConf(Path config) {
    super(config);
  }

  public WritableJobConf(String config) {
    super(config);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    if(in instanceof InputStream) {
      this.addResource((InputStream)in);
    } else {
      this.addResource(new  InputStream() {
        @Override
        public int read() throws IOException {
          return in.readByte();
        }
      });
    }
    
    this.get("foo"); //so that getProps() is called, before returining back.
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    if(out instanceof OutputStream) {
      write((OutputStream)out);
    }
    else {
      write(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          out.writeByte(b);
        }
      });
    }
  }

}
