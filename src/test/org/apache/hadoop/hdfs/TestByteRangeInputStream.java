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
package org.apache.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.junit.Test;

public class TestByteRangeInputStream {
public static class MockHttpURLConnection extends HttpURLConnection {
  public MockHttpURLConnection(URL u) {
    super(u);
  }
  
  @Override
  public boolean usingProxy(){
    return false;
  }
  
  @Override
  public void disconnect() {
  }
  
  @Override
  public void connect() {
  }
  
  @Override
  public InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream("asdf".getBytes());
  } 

  @Override
  public URL getURL() {
    URL u = null;
    try {
      u = new URL("http://resolvedurl/");
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return u;
  }
  
  @Override
  public int getResponseCode() {
    if (responseCode != -1) {
      return responseCode;
    } else {
      if (getRequestProperty("Range") == null) {
        return 200;
      } else {
        return 206;
      }
    }
  }

  public void setResponseCode(int resCode) {
    responseCode = resCode;
  }
}

  @Test
  public void testByteRange() throws IOException {
  }
}
