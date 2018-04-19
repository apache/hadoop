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

import com.google.common.collect.ImmutableMap;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_NAME;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY_CELL_SIZE;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY_NAME;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY_STATE;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_SCHEMA;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_SCHEMA_CODEC_NAME;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_SCHEMA_OPTION;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestOfflineImageViewer {
  private static final Log LOG = LogFactory.getLog(OfflineImageViewerPB.class);
  private static final int NUM_DIRS = 3;
  private static final int FILES_PER_DIR = 4;
  private static final String TEST_RENEWER = "JobTracker";
  private static File originalFsimage = null;
  private static int filesECCount = 0;
  private static String addedErasureCodingPolicyName = null;

  // namespace as written to dfs, to be compared with viewer's output
  final static HashMap<String, FileStatus> writtenFiles = Maps.newHashMap();
  static int dirCount = 0;
  private static File tempDir;

  // Create a populated namespace for later testing. Save its contents to a
  // data structure and store its fsimage location.
  // We only want to generate the fsimage file once and use it for
  // multiple tests.
  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    File[] nnDirs = MiniDFSCluster.getNameNodeDirectory(
        MiniDFSCluster.getBaseDirectory(), 0, 0);
    tempDir = nnDirs[0];

    MiniDFSCluster cluster = null;
    try {
      final ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies
          .getByID(SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);

      Configuration conf = new Configuration();
      conf.setLong(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
      conf.setLong(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 5000);
      conf.setBoolean(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
          "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//" + "DEFAULT");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      hdfs.enableErasureCodingPolicy(ecPolicy.getName());

      Map<String, String> options = ImmutableMap.of("k1", "v1", "k2", "v2");
      ECSchema schema = new ECSchema(ErasureCodeConstants.RS_CODEC_NAME,
          10, 4, options);
      ErasureCodingPolicy policy = new ErasureCodingPolicy(schema, 1024);
      AddErasureCodingPolicyResponse[] responses =
          hdfs.addErasureCodingPolicies(new ErasureCodingPolicy[]{policy});
      addedErasureCodingPolicyName = responses[0].getPolicy().getName();
      hdfs.enableErasureCodingPolicy(addedErasureCodingPolicyName);

      // Create a reasonable namespace
      for (int i = 0; i < NUM_DIRS; i++, dirCount++) {
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

      // Create an empty directory
      Path emptydir = new Path("/emptydir");
      hdfs.mkdirs(emptydir);
      dirCount++;
      writtenFiles.put(emptydir.toString(), hdfs.getFileStatus(emptydir));

      //Create directories whose name should be escaped in XML
      Path invalidXMLDir = new Path("/dirContainingInvalidXMLChar\u0000here");
      hdfs.mkdirs(invalidXMLDir);
      dirCount++;
      Path entityRefXMLDir = new Path("/dirContainingEntityRef&here");
      hdfs.mkdirs(entityRefXMLDir);
      dirCount++;
      writtenFiles.put(entityRefXMLDir.toString(),
          hdfs.getFileStatus(entityRefXMLDir));

      //Create a directory with sticky bits
      Path stickyBitDir = new Path("/stickyBit");
      hdfs.mkdirs(stickyBitDir);
      hdfs.setPermission(stickyBitDir, new FsPermission(FsAction.ALL,
          FsAction.ALL, FsAction.ALL, true));
      dirCount++;
      writtenFiles.put(stickyBitDir.toString(),
          hdfs.getFileStatus(stickyBitDir));

      // Get delegation tokens so we log the delegation token op
      Token<?>[] delegationTokens = hdfs
          .addDelegationTokens(TEST_RENEWER, null);
      for (Token<?> t : delegationTokens) {
        LOG.debug("got token " + t);
      }

      // Create INodeReference
      final Path src = new Path("/src");
      hdfs.mkdirs(src);
      dirCount++;
      writtenFiles.put(src.toString(), hdfs.getFileStatus(src));

      // Create snapshot and snapshotDiff.
      final Path orig = new Path("/src/orig");
      hdfs.mkdirs(orig);
      final Path file1 = new Path("/src/file");
      FSDataOutputStream o = hdfs.create(file1);
      o.write(23);
      o.write(45);
      o.close();
      hdfs.allowSnapshot(src);
      hdfs.createSnapshot(src, "snapshot");
      final Path dst = new Path("/dst");
      // Rename a directory in the snapshot directory to add snapshotCopy
      // field to the dirDiff entry.
      hdfs.rename(orig, dst);
      dirCount++;
      writtenFiles.put(dst.toString(), hdfs.getFileStatus(dst));
      // Truncate a file in the snapshot directory to add snapshotCopy and
      // blocks fields to the fileDiff entry.
      hdfs.truncate(file1, 1);
      writtenFiles.put(file1.toString(), hdfs.getFileStatus(file1));

      // Set XAttrs so the fsimage contains XAttr ops
      final Path xattr = new Path("/xattr");
      hdfs.mkdirs(xattr);
      dirCount++;
      hdfs.setXAttr(xattr, "user.a1", new byte[]{ 0x31, 0x32, 0x33 });
      hdfs.setXAttr(xattr, "user.a2", new byte[]{ 0x37, 0x38, 0x39 });
      // OIV should be able to handle empty value XAttrs
      hdfs.setXAttr(xattr, "user.a3", null);
      // OIV should be able to handle XAttr values that can't be expressed
      // as UTF8
      hdfs.setXAttr(xattr, "user.a4", new byte[]{ -0x3d, 0x28 });
      writtenFiles.put(xattr.toString(), hdfs.getFileStatus(xattr));
      // Set ACLs
      hdfs.setAcl(
          xattr,
          Lists.newArrayList(aclEntry(ACCESS, USER, ALL),
              aclEntry(ACCESS, USER, "foo", ALL),
              aclEntry(ACCESS, GROUP, READ_EXECUTE),
              aclEntry(ACCESS, GROUP, "bar", READ_EXECUTE),
              aclEntry(ACCESS, OTHER, EXECUTE)));

      // Create an Erasure Coded dir
      Path ecDir = new Path("/ec");
      hdfs.mkdirs(ecDir);
      dirCount++;
      hdfs.getClient().setErasureCodingPolicy(ecDir.toString(),
          ecPolicy.getName());
      writtenFiles.put(ecDir.toString(), hdfs.getFileStatus(ecDir));

      // Create an empty Erasure Coded file
      Path emptyECFile = new Path(ecDir, "EmptyECFile.txt");
      hdfs.create(emptyECFile).close();
      writtenFiles.put(emptyECFile.toString(),
          pathToFileEntry(hdfs, emptyECFile.toString()));
      filesECCount++;

      // Create a small Erasure Coded file
      Path smallECFile = new Path(ecDir, "SmallECFile.txt");
      FSDataOutputStream out = hdfs.create(smallECFile);
      Random r = new Random();
      byte[] bytes = new byte[1024 * 10];
      r.nextBytes(bytes);
      out.write(bytes);
      writtenFiles.put(smallECFile.toString(),
          pathToFileEntry(hdfs, smallECFile.toString()));
      filesECCount++;

      // Write results to the fsimage file
      hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      hdfs.saveNamespace();
      hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);

      // Determine location of fsimage file
      originalFsimage = FSImageTestUtil.findLatestImageFile(FSImageTestUtil
          .getFSImage(cluster.getNameNode()).getStorage().getStorageDir(0));
      if (originalFsimage == null) {
        throw new RuntimeException("Didn't generate or can't find fsimage");
      }
      LOG.debug("original FS image file is " + originalFsimage);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @AfterClass
  public static void deleteOriginalFSImage() throws IOException {
    FileUtils.deleteQuietly(tempDir);
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

  @Test(expected = IOException.class)
  public void testTruncatedFSImage() throws IOException {
    File truncatedFile = new File(tempDir, "truncatedFsImage");
    PrintStream output = new PrintStream(NullOutputStream.NULL_OUTPUT_STREAM);
    copyPartOfFile(originalFsimage, truncatedFile);
    new FileDistributionCalculator(new Configuration(), 0, 0, false, output)
        .visit(new RandomAccessFile(truncatedFile, "r"));
  }

  private void copyPartOfFile(File src, File dest) throws IOException {
    FileInputStream in = null;
    FileOutputStream out = null;
    final int MAX_BYTES = 700;
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);
      in.getChannel().transferTo(0, MAX_BYTES, out.getChannel());
      out.close();
      out = null;
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
  }

  @Test
  public void testFileDistributionCalculator() throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    PrintStream o = new PrintStream(output);
    new FileDistributionCalculator(new Configuration(), 0, 0, false, o)
        .visit(new RandomAccessFile(originalFsimage, "r"));
    o.close();

    String outputString = output.toString();
    Pattern p = Pattern.compile("totalFiles = (\\d+)\n");
    Matcher matcher = p.matcher(outputString);
    assertTrue(matcher.find() && matcher.groupCount() == 1);
    int totalFiles = Integer.parseInt(matcher.group(1));
    assertEquals(NUM_DIRS * FILES_PER_DIR + filesECCount + 1, totalFiles);

    p = Pattern.compile("totalDirectories = (\\d+)\n");
    matcher = p.matcher(outputString);
    assertTrue(matcher.find() && matcher.groupCount() == 1);
    int totalDirs = Integer.parseInt(matcher.group(1));
    // totalDirs includes root directory
    assertEquals(dirCount + 1, totalDirs);

    FileStatus maxFile = Collections.max(writtenFiles.values(),
        new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus first, FileStatus second) {
        return first.getLen() < second.getLen() ? -1 :
            ((first.getLen() == second.getLen()) ? 0 : 1);
      }
    });
    p = Pattern.compile("maxFileSize = (\\d+)\n");
    matcher = p.matcher(output.toString("UTF-8"));
    assertTrue(matcher.find() && matcher.groupCount() == 1);
    assertEquals(maxFile.getLen(), Long.parseLong(matcher.group(1)));
  }

  @Test
  public void testFileDistributionCalculatorWithOptions() throws Exception {
    int status = OfflineImageViewerPB.run(new String[] {"-i",
        originalFsimage.getAbsolutePath(), "-o", "-", "-p", "FileDistribution",
        "-maxSize", "512", "-step", "8"});
    assertEquals(0, status);
  }

  /**
   *  SAX handler to verify EC Files and their policies.
   */
  class ECXMLHandler extends DefaultHandler {

    private boolean isInode = false;
    private boolean isAttrRepl = false;
    private boolean isAttrName = false;
    private boolean isXAttrs = false;
    private boolean isAttrECPolicy = false;
    private boolean isAttrBlockType = false;
    private String currentInodeName;
    private String currentECPolicy;
    private String currentBlockType;
    private String currentRepl;

    @Override
    public void startElement(String uri, String localName, String qName,
        Attributes attributes) throws SAXException {
      super.startElement(uri, localName, qName, attributes);
      if (qName.equalsIgnoreCase(PBImageXmlWriter.INODE_SECTION_INODE)) {
        isInode = true;
      } else if (isInode && !isXAttrs && qName.equalsIgnoreCase(
          PBImageXmlWriter.SECTION_NAME)) {
        isAttrName = true;
      } else if (isInode && qName.equalsIgnoreCase(
          PBImageXmlWriter.SECTION_REPLICATION)) {
        isAttrRepl = true;
      } else if (isInode &&
          qName.equalsIgnoreCase(PBImageXmlWriter.INODE_SECTION_EC_POLICY_ID)) {
        isAttrECPolicy = true;
      } else if (isInode && qName.equalsIgnoreCase(
          PBImageXmlWriter.INODE_SECTION_BLOCK_TYPE)) {
        isAttrBlockType = true;
      } else if (isInode && qName.equalsIgnoreCase(
          PBImageXmlWriter.INODE_SECTION_XATTRS)) {
        isXAttrs = true;
      }
    }

    @Override
    public void endElement(String uri, String localName, String qName)
        throws SAXException {
      super.endElement(uri, localName, qName);
      if (qName.equalsIgnoreCase(PBImageXmlWriter.INODE_SECTION_INODE)) {
        if (currentInodeName != null && currentInodeName.length() > 0) {
          if (currentBlockType != null && currentBlockType.equalsIgnoreCase(
              BlockType.STRIPED.name())) {
            Assert.assertEquals("INode '"
                    + currentInodeName + "' has unexpected EC Policy!",
                Byte.parseByte(currentECPolicy),
                SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
            Assert.assertEquals("INode '"
                    + currentInodeName + "' has unexpected replication!",
                currentRepl,
                Short.toString(INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS));
          }
        }
        isInode = false;
        currentInodeName = "";
        currentECPolicy = "";
        currentRepl = "";
      } else if (qName.equalsIgnoreCase(
          PBImageXmlWriter.INODE_SECTION_XATTRS)) {
        isXAttrs = false;
      }
    }

    @Override
    public void characters(char[] ch, int start, int length)
        throws SAXException {
      super.characters(ch, start, length);
      String value = new String(ch, start, length);
      if (isAttrName) {
        currentInodeName = value;
        isAttrName = false;
      } else if (isAttrRepl) {
        currentRepl = value;
        isAttrRepl = false;
      } else if (isAttrECPolicy) {
        currentECPolicy = value;
        isAttrECPolicy = false;
      } else if (isAttrBlockType) {
        currentBlockType = value;
        isAttrBlockType = false;
      }
    }
  }

  @Test
  public void testPBImageXmlWriter() throws IOException, SAXException,
      ParserConfigurationException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    PrintStream o = new PrintStream(output);
    PBImageXmlWriter v = new PBImageXmlWriter(new Configuration(), o);
    v.visit(new RandomAccessFile(originalFsimage, "r"));
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser parser = spf.newSAXParser();
    final String xml = output.toString();
    ECXMLHandler ecxmlHandler = new ECXMLHandler();
    parser.parse(new InputSource(new StringReader(xml)), ecxmlHandler);
  }

  @Test
  public void testWebImageViewer() throws Exception {
    WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"));
    try {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)FileSystem.get(uri, conf);

      // verify the number of directories
      FileStatus[] statuses = webhdfs.listStatus(new Path("/"));
      assertEquals(dirCount, statuses.length);

      // verify the number of files in the directory
      statuses = webhdfs.listStatus(new Path("/dir0"));
      assertEquals(FILES_PER_DIR, statuses.length);

      // compare a file
      FileStatus status = webhdfs.listStatus(new Path("/dir0/file0"))[0];
      FileStatus expected = writtenFiles.get("/dir0/file0");
      compareFile(expected, status);

      // LISTSTATUS operation to an empty directory
      statuses = webhdfs.listStatus(new Path("/emptydir"));
      assertEquals(0, statuses.length);

      // LISTSTATUS operation to a invalid path
      URL url = new URL("http://localhost:" + port +
                    "/webhdfs/v1/invalid/?op=LISTSTATUS");
      verifyHttpResponseCode(HttpURLConnection.HTTP_NOT_FOUND, url);

      // LISTSTATUS operation to a invalid prefix
      url = new URL("http://localhost:" + port + "/foo");
      verifyHttpResponseCode(HttpURLConnection.HTTP_NOT_FOUND, url);

      // Verify the Erasure Coded empty file status
      Path emptyECFilePath = new Path("/ec/EmptyECFile.txt");
      FileStatus actualEmptyECFileStatus =
          webhdfs.getFileStatus(new Path(emptyECFilePath.toString()));
      FileStatus expectedEmptyECFileStatus = writtenFiles.get(
          emptyECFilePath.toString());
      System.out.println(webhdfs.getFileStatus(new Path(emptyECFilePath
              .toString())));
      compareFile(expectedEmptyECFileStatus, actualEmptyECFileStatus);

      // Verify the Erasure Coded small file status
      Path smallECFilePath = new Path("/ec/SmallECFile.txt");
      FileStatus actualSmallECFileStatus =
          webhdfs.getFileStatus(new Path(smallECFilePath.toString()));
      FileStatus expectedSmallECFileStatus = writtenFiles.get(
          smallECFilePath.toString());
      compareFile(expectedSmallECFileStatus, actualSmallECFileStatus);

      // GETFILESTATUS operation
      status = webhdfs.getFileStatus(new Path("/dir0/file0"));
      compareFile(expected, status);

      // GETFILESTATUS operation to a invalid path
      url = new URL("http://localhost:" + port +
                    "/webhdfs/v1/invalid/?op=GETFILESTATUS");
      verifyHttpResponseCode(HttpURLConnection.HTTP_NOT_FOUND, url);

      // invalid operation
      url = new URL("http://localhost:" + port + "/webhdfs/v1/?op=INVALID");
      verifyHttpResponseCode(HttpURLConnection.HTTP_BAD_REQUEST, url);

      // invalid method
      url = new URL("http://localhost:" + port + "/webhdfs/v1/?op=LISTSTATUS");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.connect();
      assertEquals(HttpURLConnection.HTTP_BAD_METHOD,
          connection.getResponseCode());
    } finally {
      // shutdown the viewer
      viewer.close();
    }
  }

  @Test
  public void testPBDelimitedWriter() throws IOException, InterruptedException {
    testPBDelimitedWriter("");  // Test in memory db.
    testPBDelimitedWriter(
        new FileSystemTestHelper().getTestRootDir() + "/delimited.db");
  }

  @Test
  public void testInvalidProcessorOption() throws Exception {
    int status =
        OfflineImageViewerPB.run(new String[] { "-i",
            originalFsimage.getAbsolutePath(), "-o", "-", "-p", "invalid" });
    assertTrue("Exit code returned for invalid processor option is incorrect",
        status != 0);
  }

  @Test
  public void testOfflineImageViewerHelpMessage() throws Throwable {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    try {
      System.setOut(out);
      int status = OfflineImageViewerPB.run(new String[] { "-h" });
      assertTrue("Exit code returned for help option is incorrect", status == 0);
      Assert.assertFalse(
          "Invalid Command error displayed when help option is passed.", bytes
              .toString().contains("Error parsing command-line options"));
      status =
          OfflineImageViewerPB.run(new String[] { "-h", "-i",
              originalFsimage.getAbsolutePath(), "-o", "-", "-p",
              "FileDistribution", "-maxSize", "512", "-step", "8" });
      Assert.assertTrue(
          "Exit code returned for help with other option is incorrect",
          status == -1);
    } finally {
      System.setOut(oldOut);
      IOUtils.closeStream(out);
    }
  }

  private void testPBDelimitedWriter(String db)
      throws IOException, InterruptedException {
    final String DELIMITER = "\t";
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    try (PrintStream o = new PrintStream(output)) {
      PBImageDelimitedTextWriter v =
          new PBImageDelimitedTextWriter(o, DELIMITER, db);
      v.visit(new RandomAccessFile(originalFsimage, "r"));
    }

    Set<String> fileNames = new HashSet<>();
    try (
        ByteArrayInputStream input =
            new ByteArrayInputStream(output.toByteArray());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(input))) {
      String line;
      boolean header = true;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        String[] fields = line.split(DELIMITER);
        assertEquals(12, fields.length);
        if (!header) {
          fileNames.add(fields[0]);
        }
        header = false;
      }
    }

    // writtenFiles does not contain root directory and "invalid XML char" dir.
    for (Iterator<String> it = fileNames.iterator(); it.hasNext(); ) {
      String filename = it.next();
      if (filename.startsWith("/dirContainingInvalidXMLChar")) {
        it.remove();
      } else if (filename.equals("/")) {
        it.remove();
      }
    }
    assertEquals(writtenFiles.keySet(), fileNames);
  }

  private static void compareFile(FileStatus expected, FileStatus status) {
    assertEquals(expected.getAccessTime(), status.getAccessTime());
    assertEquals(expected.getBlockSize(), status.getBlockSize());
    assertEquals(expected.getGroup(), status.getGroup());
    assertEquals(expected.getLen(), status.getLen());
    assertEquals(expected.getModificationTime(),
        status.getModificationTime());
    assertEquals(expected.getOwner(), status.getOwner());
    assertEquals(expected.getPermission(), status.getPermission());
    assertEquals(expected.getReplication(), status.getReplication());
    assertEquals(expected.isDirectory(), status.isDirectory());
  }

  private void verifyHttpResponseCode(int expectedCode, URL url)
      throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    connection.connect();
    assertEquals(expectedCode, connection.getResponseCode());
  }

  /**
   * Tests the ReverseXML processor.
   *
   * 1. Translate fsimage -> reverseImage.xml
   * 2. Translate reverseImage.xml -> reverseImage
   * 3. Translate reverseImage -> reverse2Image.xml
   * 4. Verify that reverseImage.xml and reverse2Image.xml match
   *
   * @throws Throwable
   */
  @Test
  public void testReverseXmlRoundTrip() throws Throwable {
    GenericTestUtils.setLogLevel(OfflineImageReconstructor.LOG,
        Level.TRACE);
    File reverseImageXml = new File(tempDir, "reverseImage.xml");
    File reverseImage =  new File(tempDir, "reverseImage");
    File reverseImage2Xml =  new File(tempDir, "reverseImage2.xml");
    LOG.info("Creating reverseImage.xml=" + reverseImageXml.getAbsolutePath() +
        ", reverseImage=" + reverseImage.getAbsolutePath() +
        ", reverseImage2Xml=" + reverseImage2Xml.getAbsolutePath());
    if (OfflineImageViewerPB.run(new String[] { "-p", "XML",
         "-i", originalFsimage.getAbsolutePath(),
         "-o", reverseImageXml.getAbsolutePath() }) != 0) {
      throw new IOException("oiv returned failure creating first XML file.");
    }
    if (OfflineImageViewerPB.run(new String[] { "-p", "ReverseXML",
          "-i", reverseImageXml.getAbsolutePath(),
          "-o", reverseImage.getAbsolutePath() }) != 0) {
      throw new IOException("oiv returned failure recreating fsimage file.");
    }
    if (OfflineImageViewerPB.run(new String[] { "-p", "XML",
        "-i", reverseImage.getAbsolutePath(),
        "-o", reverseImage2Xml.getAbsolutePath() }) != 0) {
      throw new IOException("oiv returned failure creating second " +
          "XML file.");
    }
    // The XML file we wrote based on the re-created fsimage should be the
    // same as the one we dumped from the original fsimage.
    Assert.assertEquals("",
      GenericTestUtils.getFilesDiff(reverseImageXml, reverseImage2Xml));
  }

  /**
   * Tests that the ReverseXML processor doesn't accept XML files with the wrong
   * layoutVersion.
   */
  @Test
  public void testReverseXmlWrongLayoutVersion() throws Throwable {
    File imageWrongVersion = new File(tempDir, "imageWrongVersion.xml");
    PrintWriter writer = new PrintWriter(imageWrongVersion, "UTF-8");
    try {
      writer.println("<?xml version=\"1.0\"?>");
      writer.println("<fsimage>");
      writer.println("<version>");
      writer.println(String.format("<layoutVersion>%d</layoutVersion>",
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION + 1));
      writer.println("<onDiskVersion>1</onDiskVersion>");
      writer.println("<oivRevision>" +
          "545bbef596c06af1c3c8dca1ce29096a64608478</oivRevision>");
      writer.println("</version>");
      writer.println("</fsimage>");
    } finally {
      writer.close();
    }
    try {
      OfflineImageReconstructor.run(imageWrongVersion.getAbsolutePath(),
          imageWrongVersion.getAbsolutePath() + ".out"); 
      Assert.fail("Expected OfflineImageReconstructor to fail with " +
          "version mismatch.");
    } catch (Throwable t) {
      GenericTestUtils.assertExceptionContains("Layout version mismatch.", t);
    }
  }

  @Test
  public void testFileDistributionCalculatorForException() throws Exception {
    File fsimageFile = null;
    Configuration conf = new Configuration();
    HashMap<String, FileStatus> files = Maps.newHashMap();

    // Create a initial fsimage file
    try (MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();

      // Create a reasonable namespace
      Path dir = new Path("/dir");
      hdfs.mkdirs(dir);
      files.put(dir.toString(), pathToFileEntry(hdfs, dir.toString()));
      // Create files with byte size that can't be divided by step size,
      // the byte size for here are 3, 9, 15, 21.
      for (int i = 0; i < FILES_PER_DIR; i++) {
        Path file = new Path(dir, "file" + i);
        DFSTestUtil.createFile(hdfs, file, 6 * i + 3, (short) 1, 0);

        files.put(file.toString(),
            pathToFileEntry(hdfs, file.toString()));
      }

      // Write results to the fsimage file
      hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      hdfs.saveNamespace();
      // Determine location of fsimage file
      fsimageFile =
          FSImageTestUtil.findLatestImageFile(FSImageTestUtil
              .getFSImage(cluster.getNameNode()).getStorage().getStorageDir(0));
      if (fsimageFile == null) {
        throw new RuntimeException("Didn't generate or can't find fsimage");
      }
    }

    // Run the test with params -maxSize 23 and -step 4, it will not throw
    // ArrayIndexOutOfBoundsException with index 6 when deals with
    // 21 byte size file.
    int status =
        OfflineImageViewerPB.run(new String[] {"-i",
            fsimageFile.getAbsolutePath(), "-o", "-", "-p",
            "FileDistribution", "-maxSize", "23", "-step", "4"});
    assertEquals(0, status);
  }

  @Test
  public void testOfflineImageViewerMaxSizeAndStepOptions() throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    try {
      System.setOut(out);
      // Add the -h option to make the test only for option parsing,
      // and don't need to do the following operations.
      OfflineImageViewer.main(new String[] {"-i", "-", "-o", "-", "-p",
          "FileDistribution", "-maxSize", "512", "-step", "8", "-h"});
      Assert.assertFalse(bytes.toString().contains(
          "Error parsing command-line options: "));
    } finally {
      System.setOut(oldOut);
      IOUtils.closeStream(out);
    }
  }

  @Test
  public void testOfflineImageViewerWithFormatOption() throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    try {
      System.setOut(out);
      int status =
          OfflineImageViewerPB.run(new String[] {"-i",
              originalFsimage.getAbsolutePath(), "-o", "-", "-p",
              "FileDistribution", "-maxSize", "512", "-step", "8",
              "-format"});
      assertEquals(0, status);
      Assert.assertTrue(bytes.toString().contains("(0 B, 8 B]"));
    } finally {
      System.setOut(oldOut);
      IOUtils.closeStream(out);
    }
  }

  private static String getXmlString(Element element, String name) {
    NodeList id = element.getElementsByTagName(name);
    Element line = (Element) id.item(0);
    if (line == null) {
      return "";
    }
    Node first = line.getFirstChild();
    // handle empty <key></key>
    if (first == null) {
      return "";
    }
    String val = first.getNodeValue();
    if (val == null) {
      return "";
    }
    return val;
  }

  @Test
  public void testOfflineImageViewerForECPolicies() throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    PrintStream o = new PrintStream(output);
    PBImageXmlWriter v = new PBImageXmlWriter(new Configuration(), o);
    v.visit(new RandomAccessFile(originalFsimage, "r"));
    final String xml = output.toString();

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList ecSection = dom.getElementsByTagName(ERASURE_CODING_SECTION_NAME);
    assertEquals(1, ecSection.getLength());
    NodeList policies =
        dom.getElementsByTagName(ERASURE_CODING_SECTION_POLICY);
    assertEquals(1 + SystemErasureCodingPolicies.getPolicies().size(),
        policies.getLength());
    for (int i = 0; i < policies.getLength(); i++) {
      Element policy = (Element) policies.item(i);
      String name = getXmlString(policy, ERASURE_CODING_SECTION_POLICY_NAME);
      if (name.equals(addedErasureCodingPolicyName)) {
        String cellSize =
            getXmlString(policy, ERASURE_CODING_SECTION_POLICY_CELL_SIZE);
        assertEquals("1024", cellSize);
        String state =
            getXmlString(policy, ERASURE_CODING_SECTION_POLICY_STATE);
        assertEquals(ErasureCodingPolicyState.ENABLED.toString(), state);

        Element schema = (Element) policy
            .getElementsByTagName(ERASURE_CODING_SECTION_SCHEMA).item(0);
        String codecName =
            getXmlString(schema, ERASURE_CODING_SECTION_SCHEMA_CODEC_NAME);
        assertEquals(ErasureCodeConstants.RS_CODEC_NAME, codecName);

        NodeList options =
            schema.getElementsByTagName(ERASURE_CODING_SECTION_SCHEMA_OPTION);
        assertEquals(2, options.getLength());
        Element option1 = (Element) options.item(0);
        assertEquals("k1", getXmlString(option1, "key"));
        assertEquals("v1", getXmlString(option1, "value"));
        Element option2 = (Element) options.item(1);
        assertEquals("k2", getXmlString(option2, "key"));
        assertEquals("v2", getXmlString(option2, "value"));
      }
    }
  }
}
