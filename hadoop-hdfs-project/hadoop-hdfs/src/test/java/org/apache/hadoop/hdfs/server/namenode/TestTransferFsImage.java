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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.HttpServerFunctionalTest;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;


public class TestTransferFsImage {

  private static final File TEST_DIR = PathUtils.getTestDir(TestTransferFsImage.class);

  /**
   * Regression test for HDFS-1997. Test that, if an exception
   * occurs on the client side, it is properly reported as such,
   * and reported to the associated NNStorage object.
   */
  @Test
  public void testClientSideException() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0).build();
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    List<File> localPath = Collections.singletonList(
        new File("/xxxxx-does-not-exist/blah"));
       
    try {
      URL fsName = DFSUtil.getInfoServer(
          cluster.getNameNode().getServiceRpcAddress(), conf,
          DFSUtil.getHttpClientScheme(conf)).toURL();
      String id = "getimage=1&txid=0";

      TransferFsImage.getFileClient(fsName, id, localPath, mockStorage, false);      
      fail("Didn't get an exception!");
    } catch (IOException ioe) {
      Mockito.verify(mockStorage).reportErrorOnFile(localPath.get(0));
      assertTrue(
          "Unexpected exception: " + StringUtils.stringifyException(ioe),
          ioe.getMessage().contains("Unable to download to any storage"));
    } finally {
      cluster.shutdown();      
    }
  }
  
  /**
   * Similar to the above test, except that there are multiple local files
   * and one of them can be saved.
   */
  @Test
  public void testClientSideExceptionOnJustOneDir() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0).build();
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    List<File> localPaths = ImmutableList.of(
        new File("/xxxxx-does-not-exist/blah"),
        new File(TEST_DIR, "testfile")    
        );
       
    try {
      URL fsName = DFSUtil.getInfoServer(
          cluster.getNameNode().getServiceRpcAddress(), conf,
          DFSUtil.getHttpClientScheme(conf)).toURL();

      String id = "getimage=1&txid=0";

      TransferFsImage.getFileClient(fsName, id, localPaths, mockStorage, false);      
      Mockito.verify(mockStorage).reportErrorOnFile(localPaths.get(0));
      assertTrue("The valid local file should get saved properly",
          localPaths.get(1).length() > 0);
    } finally {
      cluster.shutdown();      
    }
  }

  /**
   * Test to verify the read timeout
   */
  @Test(timeout = 10000)
  public void testGetImageTimeout() throws Exception {
    HttpServer2 testServer = HttpServerFunctionalTest.createServer("hdfs");
    try {
      testServer.addServlet("ImageTransfer", ImageServlet.PATH_SPEC,
          TestImageTransferServlet.class);
      testServer.start();
      URL serverURL = HttpServerFunctionalTest.getServerURL(testServer);
      TransferFsImage.timeout = 2000;
      try {
        TransferFsImage.getFileClient(serverURL, "txid=1", null,
            null, false);
        fail("TransferImage Should fail with timeout");
      } catch (SocketTimeoutException e) {
        assertEquals("Read should timeout", "Read timed out", e.getMessage());
      }
    } finally {
      if (testServer != null) {
        testServer.stop();
      }
    }
  }

  /**
   * Test to verify the timeout of Image upload
   */
  @Test(timeout = 10000)
  public void testImageUploadTimeout() throws Exception {
    Configuration conf = new HdfsConfiguration();
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    HttpServer2 testServer = HttpServerFunctionalTest.createServer("hdfs");
    try {
      testServer.addServlet("ImageTransfer", ImageServlet.PATH_SPEC,
          TestImageTransferServlet.class);
      testServer.start();
      URL serverURL = HttpServerFunctionalTest.getServerURL(testServer);
      // set the timeout here, otherwise it will take default.
      TransferFsImage.timeout = 2000;

      File tmpDir = new File(new FileSystemTestHelper().getTestRootDir());
      tmpDir.mkdirs();

      File mockImageFile = File.createTempFile("image", "", tmpDir);
      FileOutputStream imageFile = new FileOutputStream(mockImageFile);
      imageFile.write("data".getBytes());
      imageFile.close();
      Mockito.when(
          mockStorage.findImageFile(Mockito.any(NameNodeFile.class),
              Mockito.anyLong())).thenReturn(mockImageFile);
      Mockito.when(mockStorage.toColonSeparatedString()).thenReturn(
          "storage:info:string");
      
      try {
        TransferFsImage.uploadImageFromStorage(serverURL, conf, mockStorage,
            NameNodeFile.IMAGE, 1L);
        fail("TransferImage Should fail with timeout");
      } catch (SocketTimeoutException e) {
        assertEquals("Upload should timeout", "Read timed out", e.getMessage());
      }
    } finally {
      testServer.stop();
    }
  }

  public static class TestImageTransferServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      synchronized (this) {
        try {
          wait(5000);
        } catch (InterruptedException e) {
          // Ignore
        }
      }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      synchronized (this) {
        try {
          wait(5000);
        } catch (InterruptedException e) {
          // Ignore
        }
      }
    }
  }
}
