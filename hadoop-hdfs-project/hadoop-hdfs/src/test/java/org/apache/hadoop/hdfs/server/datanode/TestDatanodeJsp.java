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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspWriter;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ServletUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDatanodeJsp {
  
  private static final String FILE_DATA = "foo bar baz biz buz";
  private static final HdfsConfiguration CONF = new HdfsConfiguration();
  private static String viewFilePage;
  
  private static void testViewingFile(MiniDFSCluster cluster, String filePath)
      throws IOException {
    FileSystem fs = cluster.getFileSystem();
    
    Path testPath = new Path(filePath);
    if (!fs.exists(testPath)) {
      DFSTestUtil.writeFile(fs, testPath, FILE_DATA);
    }
    
    InetSocketAddress nnIpcAddress = cluster.getNameNode().getNameNodeAddress();
    InetSocketAddress nnHttpAddress = cluster.getNameNode().getHttpAddress();
    String base = JspHelper.Url.url("http", cluster.getDataNodes().get(0)
        .getDatanodeId());

    URL url = new URL(base + "/"
        + "browseDirectory.jsp" + JspHelper.getUrlParam("dir", 
            URLEncoder.encode(testPath.toString(), "UTF-8"), true)
        + JspHelper.getUrlParam("namenodeInfoPort", Integer
            .toString(nnHttpAddress.getPort())) + JspHelper
            .getUrlParam("nnaddr", "localhost:" + nnIpcAddress.getPort()));
    
    viewFilePage = StringEscapeUtils.unescapeHtml(DFSTestUtil.urlGet(url));
    
    assertTrue("page should show preview of file contents, got: " + viewFilePage,
        viewFilePage.contains(FILE_DATA));
    
    assertTrue("page should show link to download file", viewFilePage
        .contains("/streamFile" + ServletUtil.encodePath(filePath)
            + "?nnaddr=localhost:" + nnIpcAddress.getPort()));
    
    // check whether able to tail the file
    String regex = "<a.+href=\"(.+?)\">Tail\\s*this\\s*file\\<\\/a\\>";
    assertFileContents(regex, "Tail this File");
    
    // check whether able to 'Go Back to File View' after tailing the file
    regex = "<a.+href=\"(.+?)\">Go\\s*Back\\s*to\\s*File\\s*View\\<\\/a\\>";
    assertFileContents(regex, "Go Back to File View");

    regex = "<a href=\"///" + nnHttpAddress.getHostName() + ":" +
      nnHttpAddress.getPort() + "/dfshealth.jsp\">Go back to DFS home</a>";
    assertTrue("page should generate DFS home scheme without explicit scheme", viewFilePage.contains(regex));
  }
  
  private static void assertFileContents(String regex, String text)
      throws IOException {
    Pattern compile = Pattern.compile(regex);
    Matcher matcher = compile.matcher(viewFilePage);
    if (matcher.find()) {
      // got hyperlink for Tail this file
      String u = matcher.group(1);
      String urlString = u.startsWith("///") ? ("http://" + u.substring(3)) : u;
      viewFilePage = StringEscapeUtils.unescapeHtml(DFSTestUtil
          .urlGet(new URL(urlString)));
      assertTrue("page should show preview of file contents", viewFilePage
          .contains(FILE_DATA));
    } else {
      fail(text + " hyperlink should be there in the page content : "
          + viewFilePage);
    }
  }
  
  @Test
  public void testViewFileJsp() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(CONF).build();
      cluster.waitActive();
      String paths[] = {
        "/test-file",
        "/tmp/test-file",
        "/tmp/test-file%with goofy&characters",
        "/foo bar/foo bar",
        "/foo+bar/foo+bar",
        "/foo;bar/foo;bar",
        "/foo=bar/foo=bar",
        "/foo,bar/foo,bar",
        "/foo?bar/foo?bar",
        "/foo\">bar/foo\">bar"
      };
      for (String p : paths) {
        testViewingFile(cluster, p);
        testViewingFile(cluster, p);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testGenStamp() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(1)
        .build();
    try {
      FileSystem fs = cluster.getFileSystem();
      Path testFile = new Path("/test/mkdirs/TestchunkSizeToView");
      writeFile(fs, testFile);
      JspWriter writerMock = Mockito.mock(JspWriter.class);
      HttpServletRequest reqMock = Mockito.mock(HttpServletRequest.class);
      setTheMockExpectationsFromReq(testFile, reqMock);
      DatanodeJspHelper.generateFileDetails(writerMock, reqMock, CONF);
      Mockito.verify(writerMock, Mockito.atLeastOnce()).print(
          "<input type=\"hidden\" name=\"genstamp\" value=\"987654321\">");
    } finally {
      cluster.shutdown();
    }
  }

  private void setTheMockExpectationsFromReq(Path testFile,
      HttpServletRequest reqMock) {
    Mockito.doReturn("987654321").when(reqMock).getParameter("genstamp");
    Mockito.doReturn("1234").when(reqMock).getParameter("blockId");
    Mockito.doReturn("8081").when(reqMock).getParameter("datanodePort");
    Mockito.doReturn("8080").when(reqMock).getParameter("namenodeInfoPort");
    Mockito.doReturn("100").when(reqMock).getParameter("chunkSizeToView");
    Mockito.doReturn("1").when(reqMock).getParameter("startOffset");
    Mockito.doReturn("1024").when(reqMock).getParameter("blockSize");
    Mockito.doReturn(NetUtils.getHostPortString(NameNode.getAddress(CONF)))
        .when(reqMock).getParameter("nnaddr");
    Mockito.doReturn(testFile.toString()).when(reqMock).getPathInfo();
    Mockito.doReturn("http").when(reqMock).getScheme();
  }

  static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    try {
      out.writeBytes("umamahesh: " + f);
    } finally {
      out.close();
    }
    assertTrue(fs.exists(f));
    return f;
  }

}
