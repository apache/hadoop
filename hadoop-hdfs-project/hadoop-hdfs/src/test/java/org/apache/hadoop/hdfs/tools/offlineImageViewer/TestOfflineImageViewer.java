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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Maps;

/**
 * Test function of OfflineImageViewer by: * confirming it can correctly process
 * a valid fsimage file and that the processing generates a correct
 * representation of the namespace * confirming it correctly fails to process an
 * fsimage file with a layout version it shouldn't be able to handle * confirm
 * it correctly bails on malformed image files, in particular, a file that ends
 * suddenly.
 */
public class TestOfflineImageViewer {
  private static final Log LOG = LogFactory.getLog(OfflineImageViewer.class);
  private static final int NUM_DIRS = 3;
  private static final int FILES_PER_DIR = 4;
  private static final String TEST_RENEWER = "JobTracker";
  private static File originalFsimage = null;

  // Elements of lines of ls-file output to be compared to FileStatus instance
  private static final class LsElements {
    private String perms;
    private int replication;
    private String username;
    private String groupname;
    private long filesize;
    private boolean isDir;
  }

  // namespace as written to dfs, to be compared with viewer's output
  final static HashMap<String, FileStatus> writtenFiles = Maps.newHashMap();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  // Create a populated namespace for later testing. Save its contents to a
  // data structure and store its fsimage location.
  // We only want to generate the fsimage file once and use it for
  // multiple tests.
  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setLong(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
      conf.setLong(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 5000);
      conf.setBoolean(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
          "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//" + "DEFAULT");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();

      // Create a reasonable namespace
      for (int i = 0; i < NUM_DIRS; i++) {
        Path dir = new Path("/dir" + i);
        hdfs.mkdirs(dir);
        writtenFiles.put(dir.toString(), pathToFileEntry(hdfs, dir.toString()));
        for (int j = 0; j < FILES_PER_DIR; j++) {
          Path file = new Path(dir, "file" + j);
          FSDataOutputStream o = hdfs.create(file);
          o.write(23);
          o.close();

          writtenFiles.put(file.toString(),
              pathToFileEntry(hdfs, file.toString()));
        }
      }

      // Get delegation tokens so we log the delegation token op
      Token<?>[] delegationTokens = hdfs
          .addDelegationTokens(TEST_RENEWER, null);
      for (Token<?> t : delegationTokens) {
        LOG.debug("got token " + t);
      }

      final Path snapshot = new Path("/snapshot");
      hdfs.mkdirs(snapshot);
      hdfs.allowSnapshot(snapshot);
      hdfs.mkdirs(new Path("/snapshot/1"));
      hdfs.delete(snapshot, true);

      // Write results to the fsimage file
      hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      hdfs.saveNamespace();

      // Determine location of fsimage file
      originalFsimage = FSImageTestUtil.findLatestImageFile(FSImageTestUtil
          .getFSImage(cluster.getNameNode()).getStorage().getStorageDir(0));
      if (originalFsimage == null) {
        throw new RuntimeException("Didn't generate or can't find fsimage");
      }
      LOG.debug("original FS image file is " + originalFsimage);
    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  @AfterClass
  public static void deleteOriginalFSImage() throws IOException {
    if (originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }

  // Convenience method to generate a file status from file system for
  // later comparison
  private static FileStatus pathToFileEntry(FileSystem hdfs, String file)
      throws IOException {
    return hdfs.getFileStatus(new Path(file));
  }

  // Verify that we can correctly generate an ls-style output for a valid
  // fsimage
  @Test
  public void outputOfLSVisitor() throws IOException {
    StringWriter output = new StringWriter();
    PrintWriter out = new PrintWriter(output);
    LsrPBImage v = new LsrPBImage(new Configuration(), out);
    v.visit(new RandomAccessFile(originalFsimage, "r"));
    out.close();
    Pattern pattern = Pattern
        .compile("([d\\-])([rwx\\-]{9})\\s*(-|\\d+)\\s*(\\w+)\\s*(\\w+)\\s*(\\d+)\\s*(\\d+)\\s*([\b/]+)");
    int count = 0;
    for (String s : output.toString().split("\n")) {
      Matcher m = pattern.matcher(s);
      assertTrue(m.find());
      LsElements e = new LsElements();
      e.isDir = m.group(1).equals("d");
      e.perms = m.group(2);
      e.replication = m.group(3).equals("-") ? 0 : Integer.parseInt(m.group(3));
      e.username = m.group(4);
      e.groupname = m.group(5);
      e.filesize = Long.parseLong(m.group(7));
      String path = m.group(8);
      if (!path.equals("/")) {
        compareFiles(writtenFiles.get(path), e);
      }
      ++count;
    }
    assertEquals(writtenFiles.size() + 1, count);
  }

  @Test(expected = IOException.class)
  public void testTruncatedFSImage() throws IOException {
    File truncatedFile = folder.newFile();
    StringWriter output = new StringWriter();
    copyPartOfFile(originalFsimage, truncatedFile);
    new FileDistributionCalculator(new Configuration(), 0, 0, new PrintWriter(
        output)).visit(new RandomAccessFile(truncatedFile, "r"));
  }

  // Compare two files as listed in the original namespace FileStatus and
  // the output of the ls file from the image processor
  private void compareFiles(FileStatus fs, LsElements elements) {
    assertEquals("directory listed as such", fs.isDirectory(), elements.isDir);
    assertEquals("perms string equal", fs.getPermission().toString(),
        elements.perms);
    assertEquals("replication equal", fs.getReplication(), elements.replication);
    assertEquals("owner equal", fs.getOwner(), elements.username);
    assertEquals("group equal", fs.getGroup(), elements.groupname);
    assertEquals("lengths equal", fs.getLen(), elements.filesize);
  }

  private void copyPartOfFile(File src, File dest) throws IOException {
    FileInputStream in = null;
    FileOutputStream out = null;
    final int MAX_BYTES = 700;
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);
      in.getChannel().transferTo(0, MAX_BYTES, out.getChannel());
    } finally {
      IOUtils.cleanup(null, in);
      IOUtils.cleanup(null, out);
    }
  }

  @Test
  public void testFileDistributionCalculator() throws IOException {
    StringWriter output = new StringWriter();
    PrintWriter o = new PrintWriter(output);
    new FileDistributionCalculator(new Configuration(), 0, 0, o)
        .visit(new RandomAccessFile(originalFsimage, "r"));
    o.close();

    Pattern p = Pattern.compile("totalFiles = (\\d+)\n");
    Matcher matcher = p.matcher(output.getBuffer());
    assertTrue(matcher.find() && matcher.groupCount() == 1);
    int totalFiles = Integer.parseInt(matcher.group(1));
    assertEquals(NUM_DIRS * FILES_PER_DIR, totalFiles);

    p = Pattern.compile("totalDirectories = (\\d+)\n");
    matcher = p.matcher(output.getBuffer());
    assertTrue(matcher.find() && matcher.groupCount() == 1);
    int totalDirs = Integer.parseInt(matcher.group(1));
    // totalDirs includes root directory
    assertEquals(NUM_DIRS + 1, totalDirs);

    FileStatus maxFile = Collections.max(writtenFiles.values(),
        new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus first, FileStatus second) {
        return first.getLen() < second.getLen() ? -1 :
            ((first.getLen() == second.getLen()) ? 0 : 1);
      }
    });
    p = Pattern.compile("maxFileSize = (\\d+)\n");
    matcher = p.matcher(output.getBuffer());
    assertTrue(matcher.find() && matcher.groupCount() == 1);
    assertEquals(maxFile.getLen(), Long.parseLong(matcher.group(1)));
  }

  @Test
  public void testFileDistributionCalculatorWithOptions() throws IOException {
    int status = OfflineImageViewerPB.run(new String[] {"-i",
        originalFsimage.getAbsolutePath(), "-o", "-", "-p", "FileDistribution",
        "-maxSize", "512", "-step", "8"});
    assertEquals(0, status);
  }

  @Test
  public void testPBImageXmlWriter() throws IOException, SAXException,
      ParserConfigurationException {
    StringWriter output = new StringWriter();
    PrintWriter o = new PrintWriter(output);
    PBImageXmlWriter v = new PBImageXmlWriter(new Configuration(), o);
    v.visit(new RandomAccessFile(originalFsimage, "r"));
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser parser = spf.newSAXParser();
    final String xml = output.getBuffer().toString();
    parser.parse(new InputSource(new StringReader(xml)), new DefaultHandler());
  }
}
