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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;
import org.mockito.Mockito;
import org.mortbay.jetty.InclusiveByteRange;

/*
 * Mock input stream class that always outputs the current position of the stream. 
 */
class MockFSInputStream extends FSInputStream {
  long currentPos = 0;
  @Override
  public int read() throws IOException {
    return (int)(currentPos++);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void seek(long pos) throws IOException {
    currentPos = pos;
  }
  
  @Override
  public long getPos() throws IOException {
    return currentPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}


public class TestStreamFile {
  private final HdfsConfiguration CONF = new HdfsConfiguration();
  private final DFSClient clientMock = Mockito.mock(DFSClient.class);
  private final HttpServletRequest mockHttpServletRequest =
    Mockito.mock(HttpServletRequest.class);
  private final HttpServletResponse mockHttpServletResponse =
    Mockito.mock(HttpServletResponse.class);
  private final ServletContext mockServletContext = 
    Mockito.mock(ServletContext.class);

  final StreamFile sfile = new StreamFile() {
    private static final long serialVersionUID = -5513776238875189473L;
  
    @Override
    public ServletContext getServletContext() {
      return mockServletContext;
    }
  
    @Override
    protected DFSClient getDFSClient(HttpServletRequest request)
      throws IOException, InterruptedException {
      return clientMock;
    }
  };
     
  // return an array matching the output of mockfsinputstream
  private static byte[] getOutputArray(int start, int count) {
    byte[] a = new byte[count];
    
    for (int i = 0; i < count; i++) {
      a[i] = (byte)(start+i);
    }

    return a;
  }
  
  @Test
  public void testWriteTo() throws IOException {

    FSDataInputStream fsdin = new FSDataInputStream(new MockFSInputStream());
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    // new int[]{s_1, c_1, s_2, c_2, ..., s_n, c_n} means to test
    // reading c_i bytes starting at s_i
    int[] pairs = new int[]{ 0, 10000,
                             50, 100,
                             50, 6000,
                             1000, 2000,
                             0, 1,
                             0, 0,
                             5000, 0,
                            };
                            
    assertTrue("Pairs array must be even", pairs.length % 2 == 0);
    
    for (int i = 0; i < pairs.length; i+=2) {
      StreamFile.copyFromOffset(fsdin, os, pairs[i], pairs[i+1]);
      assertArrayEquals("Reading " + pairs[i+1]
                        + " bytes from offset " + pairs[i],
                        getOutputArray(pairs[i], pairs[i+1]),
                        os.toByteArray());
      os.reset();
    }
    
  }

  @SuppressWarnings("unchecked")
  private List<InclusiveByteRange> strToRanges(String s, int contentLength) {
    List<String> l = Arrays.asList(new String[]{"bytes="+s});
    Enumeration<?> e = (new Vector<String>(l)).elements();
    return InclusiveByteRange.satisfiableRanges(e, contentLength);
  }
  
  @Test
  public void testSendPartialData() throws IOException {
    FSDataInputStream in = new FSDataInputStream(new MockFSInputStream());
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    // test if multiple ranges, then 416
    { 
      List<InclusiveByteRange> ranges = strToRanges("0-,10-300", 500);
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      StreamFile.sendPartialData(in, os, response, 500, ranges);

      // Multiple ranges should result in a 416 error
      Mockito.verify(response).setStatus(416);
    }
                              
    // test if no ranges, then 416
    { 
      os.reset();
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      StreamFile.sendPartialData(in, os, response, 500, null);

      // No ranges should result in a 416 error
      Mockito.verify(response).setStatus(416);
    }

    // test if invalid single range (out of bounds), then 416
    { 
      List<InclusiveByteRange> ranges = strToRanges("600-800", 500);
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      StreamFile.sendPartialData(in, os, response, 500, ranges);

      // Single (but invalid) range should result in a 416
      Mockito.verify(response).setStatus(416);
    }

      
    // test if one (valid) range, then 206
    { 
      List<InclusiveByteRange> ranges = strToRanges("100-300", 500);
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      StreamFile.sendPartialData(in, os, response, 500, ranges);

      // Single (valid) range should result in a 206
      Mockito.verify(response).setStatus(206);

      assertArrayEquals("Byte range from 100-300",
                        getOutputArray(100, 201),
                        os.toByteArray());
    }
    
  }
  
  
  // Test for positive scenario
  @Test
  public void testDoGetShouldWriteTheFileContentIntoServletOutputStream()
      throws Exception {

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(1)
        .build();
    try {
      Path testFile = createFile();
      setUpForDoGetTest(cluster, testFile);
      ServletOutputStreamExtn outStream = new ServletOutputStreamExtn();
      Mockito.doReturn(outStream).when(mockHttpServletResponse)
          .getOutputStream();
      StreamFile sfile = new StreamFile() {

        private static final long serialVersionUID = 7715590481809562722L;

        @Override
        public ServletContext getServletContext() {
          return mockServletContext;
        }
      };
      sfile.doGet(mockHttpServletRequest, mockHttpServletResponse);
      assertEquals("Not writing the file data into ServletOutputStream",
          outStream.getResult(), "test");
    } finally {
      cluster.shutdown();
    }
  }

  // Test for cleaning the streams in exception cases also
  @Test
  public void testDoGetShouldCloseTheDFSInputStreamIfResponseGetOutPutStreamThrowsAnyException()
      throws Exception {

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(1)
        .build();
    try {
      Path testFile = createFile();

      setUpForDoGetTest(cluster, testFile);

      Mockito.doThrow(new IOException()).when(mockHttpServletResponse)
          .getOutputStream();
      DFSInputStream fsMock = Mockito.mock(DFSInputStream.class);

      Mockito.doReturn(fsMock).when(clientMock).open(testFile.toString());

      Mockito.doReturn(Long.valueOf(4)).when(fsMock).getFileLength();

      try {
        sfile.doGet(mockHttpServletRequest, mockHttpServletResponse);
        fail("Not throwing the IOException");
      } catch (IOException e) {
        Mockito.verify(clientMock, Mockito.atLeastOnce()).close();
      }

    } finally {
      cluster.shutdown();
    }
  }

  private void setUpForDoGetTest(MiniDFSCluster cluster, Path testFile) {

    Mockito.doReturn(CONF).when(mockServletContext).getAttribute(
        JspHelper.CURRENT_CONF);
    Mockito.doReturn(NetUtils.getHostPortString(NameNode.getAddress(CONF)))
      .when(mockHttpServletRequest).getParameter("nnaddr");
    Mockito.doReturn(testFile.toString()).when(mockHttpServletRequest)
      .getPathInfo();
    Mockito.doReturn("/streamFile"+testFile.toString()).when(mockHttpServletRequest)
      .getRequestURI();
  }

  static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    try {
      out.writeBytes("test");
    } finally {
      out.close();
    }
    assertTrue(fs.exists(f));
    return f;
  }

  private Path createFile() throws IOException {
    FileSystem fs = FileSystem.get(CONF);
    Path testFile = new Path("/test/mkdirs/doGet");
    writeFile(fs, testFile);
    return testFile;
  }

  public static class ServletOutputStreamExtn extends ServletOutputStream {
    private final StringBuffer buffer = new StringBuffer(3);

    public String getResult() {
      return buffer.toString();
    }

    @Override
    public void write(int b) throws IOException {
      buffer.append((char) b);
    }
  }
}
