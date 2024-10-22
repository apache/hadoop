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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrCompactProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.XAttrFormat;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.XMLUtils;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_NAME;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY_CELL_SIZE;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY_NAME;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_POLICY_STATE;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_SCHEMA;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_SCHEMA_CODEC_NAME;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.ERASURE_CODING_SECTION_SCHEMA_OPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestOfflineImageViewer {
  private static final Logger LOG =
      LoggerFactory.getLogger(OfflineImageViewerPB.class);
  private static final int NUM_DIRS = 3;
  private static final int FILES_PER_DIR = 4;
  private static final String TEST_RENEWER = "JobTracker";
  private static File originalFsimage = null;
  private static int filesECCount = 0;
  private static String addedErasureCodingPolicyName = null;
  private static final long FILE_NODE_ID_1 = 16388;
  private static final long FILE_NODE_ID_2 = 16389;
  private static final long FILE_NODE_ID_3 = 16394;
  private static final long DIR_NODE_ID = 16391;
  private static final long SAMPLE_TIMESTAMP = 946684800000L;
  private static TimeZone defaultTimeZone = null;

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
    defaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
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
      // fsimage with sub-section conf
      conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY, "true");
      conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY, "1");
      conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY, "4");
      conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY, "4");

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

      //Create directories with new line characters
      Path newLFDir = new Path("/dirContainingNewLineChar"
          + StringUtils.LF + "here");
      hdfs.mkdirs(newLFDir);
      dirCount++;
      writtenFiles.put("\"/dirContainingNewLineChar%x0Ahere\"",
          hdfs.getFileStatus(newLFDir));

      Path newCRLFDir = new Path("/dirContainingNewLineChar"
          + PBImageDelimitedTextWriter.CRLF + "here");
      hdfs.mkdirs(newCRLFDir);
      dirCount++;
      writtenFiles.put("\"/dirContainingNewLineChar%x0D%x0Ahere\"",
          hdfs.getFileStatus(newCRLFDir));

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

      // HDFS-14148: Create a second snapshot-enabled directory. This can cause
      // TestOfflineImageViewer#testReverseXmlRoundTrip to fail before the patch
      final Path snapshotDir2 = new Path("/snapshotDir2");
      hdfs.mkdirs(snapshotDir2);
      // Simply enable snapshot on it, no need to create one
      hdfs.allowSnapshot(snapshotDir2);
      dirCount++;
      writtenFiles.put(snapshotDir2.toString(),
          hdfs.getFileStatus(snapshotDir2));

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
      hdfs.setSafeMode(SafeModeAction.ENTER, false);
      hdfs.saveNamespace();
      hdfs.setSafeMode(SafeModeAction.LEAVE, false);

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
    if (defaultTimeZone != null) {
      TimeZone.setDefault(defaultTimeZone);
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
    PrintStream output = new PrintStream(NullOutputStream.INSTANCE);
    copyPartOfFile(originalFsimage, truncatedFile);
    try (RandomAccessFile r = new RandomAccessFile(truncatedFile, "r")) {
      new FileDistributionCalculator(new Configuration(), 0, 0, false, output)
        .visit(r);
    }
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
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream o = new PrintStream(output);
        RandomAccessFile r = new RandomAccessFile(originalFsimage, "r")) {
      new FileDistributionCalculator(new Configuration(), 0, 0, false, o)
        .visit(r);
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
              return first.getLen() < second.getLen() ?
                  -1 :
                  ((first.getLen() == second.getLen()) ? 0 : 1);
            }
          });
      p = Pattern.compile("maxFileSize = (\\d+)\n");
      matcher = p.matcher(output.toString("UTF-8"));
      assertTrue(matcher.find() && matcher.groupCount() == 1);
      assertEquals(maxFile.getLen(), Long.parseLong(matcher.group(1)));
    }
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
    try (RandomAccessFile r = new RandomAccessFile(originalFsimage, "r")) {
      v.visit(r);
    }
    SAXParserFactory spf = XMLUtils.newSecureSAXParserFactory();
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
  public void testWebImageViewerNullOp() throws Exception {
    WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"));
    try {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      // null op
      URL url = new URL("http://localhost:" + port +
          "/webhdfs/v1/");
      // should get HTTP_BAD_REQUEST. NPE gets HTTP_INTERNAL_ERROR
      verifyHttpResponseCode(HttpURLConnection.HTTP_BAD_REQUEST, url);
    } finally {
      // shutdown the viewer
      viewer.close();
    }
  }

  @Test
  public void testWebImageViewerSecureMode() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    try (WebImageViewer viewer =
        new WebImageViewer(
            NetUtils.createSocketAddr("localhost:0"), conf)) {
      RuntimeException ex = LambdaTestUtils.intercept(RuntimeException.class,
          "WebImageViewer does not support secure mode.",
          () -> viewer.start("foo"));
    } finally {
      conf.set(HADOOP_SECURITY_AUTHENTICATION, "simple");
      UserGroupInformation.setConfiguration(conf);
    }
  }

  private FsImageProto.INodeSection.INode createSampleFileInode() {
    HdfsProtos.BlockProto.Builder block =
        HdfsProtos.BlockProto.newBuilder()
            .setNumBytes(1024)
            .setBlockId(8)
            .setGenStamp(SAMPLE_TIMESTAMP);
    FsImageProto.INodeSection.AclFeatureProto.Builder acl =
        FsImageProto.INodeSection.AclFeatureProto.newBuilder()
            .addEntries(2);
    FsImageProto.INodeSection.INodeFile.Builder file =
        FsImageProto.INodeSection.INodeFile.newBuilder()
            .setReplication(5)
            .setModificationTime(SAMPLE_TIMESTAMP)
            .setAccessTime(SAMPLE_TIMESTAMP)
            .setPreferredBlockSize(1024)
            .addBlocks(block)
            .addBlocks(block)
            .addBlocks(block)
            .setAcl(acl);

    return FsImageProto.INodeSection.INode.newBuilder()
        .setType(FsImageProto.INodeSection.INode.Type.FILE)
        .setFile(file)
        .setName(ByteString.copyFromUtf8("file"))
        .setId(3)
        .build();
  }

  private FsImageProto.INodeSection.INode createSampleDirInode()
      throws IOException {
    return createSampleDirInode(false);
  }

  private FsImageProto.INodeSection.INode createSampleDirInode(
      boolean builXAttr) throws IOException {
    FsImageProto.INodeSection.AclFeatureProto.Builder acl =
        FsImageProto.INodeSection.AclFeatureProto.newBuilder()
            .addEntries(2);
    FsImageProto.INodeSection.INodeDirectory.Builder directory =
        FsImageProto.INodeSection.INodeDirectory.newBuilder()
            .setDsQuota(1000)
            .setNsQuota(700)
            .setModificationTime(SAMPLE_TIMESTAMP)
            .setAcl(acl);
    if (builXAttr) {
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      DataOutputStream dOut = new DataOutputStream(bOut);
      WritableUtils.writeString(dOut, "test-value");
      XAttr a = XAttrHelper.buildXAttr("system.hdfs", bOut.toByteArray());
      XAttrFeatureProto.Builder b = XAttrFeatureProto.newBuilder();
      XAttrCompactProto.Builder xAttrCompactBuilder = XAttrCompactProto.newBuilder();
      int v = XAttrFormat.toInt(a);
      xAttrCompactBuilder.setName(v);
      xAttrCompactBuilder.setValue(PBHelperClient.getByteString(a.getValue()));
      b.addXAttrs(xAttrCompactBuilder.build());
      directory.setXAttrs(b);
    }

    return FsImageProto.INodeSection.INode.newBuilder()
        .setType(FsImageProto.INodeSection.INode.Type.DIRECTORY)
        .setDirectory(directory)
        .setName(ByteString.copyFromUtf8("dir"))
        .setId(3)
        .build();
  }

  private FsImageProto.INodeSection.INode createSampleSymlink() {
    FsImageProto.INodeSection.INodeSymlink.Builder symlink =
        FsImageProto.INodeSection.INodeSymlink.newBuilder()
            .setModificationTime(SAMPLE_TIMESTAMP)
            .setAccessTime(SAMPLE_TIMESTAMP);

    return FsImageProto.INodeSection.INode.newBuilder()
        .setType(FsImageProto.INodeSection.INode.Type.SYMLINK)
        .setSymlink(symlink)
        .setName(ByteString.copyFromUtf8("sym"))
        .setId(5)
        .build();
  }

  private PBImageDelimitedTextWriter createDelimitedWriterSpy()
      throws IOException {
    return createDelimitedWriterSpy(false);
  }

  private PBImageDelimitedTextWriter createDelimitedWriterSpy(boolean printECPolicy)
      throws IOException {
    FsPermission fsPermission = new FsPermission(
        FsAction.ALL,
        FsAction.WRITE_EXECUTE,
        FsAction.WRITE);
    PermissionStatus permStatus = new PermissionStatus(
        "user_1",
        "group_1",
        fsPermission);

    PBImageDelimitedTextWriter writer = new
        PBImageDelimitedTextWriter(null, ",", "", false,
        printECPolicy, 1, "-", new Configuration());

    PBImageDelimitedTextWriter writerSpy = spy(writer);
    when(writerSpy.getPermission(anyLong())).thenReturn(permStatus);
    return writerSpy;
  }

  @Test
  public void testWriterOutputEntryBuilderForFile() throws IOException {
    assertEquals("/path/file,5,2000-01-01 00:00,2000-01-01 00:00," +
                "1024,3,3072,0,0,-rwx-wx-w-+,user_1,group_1",
        createDelimitedWriterSpy().getEntry("/path/",
            createSampleFileInode()));
  }

  @Test
  public void testWriterOutputEntryBuilderForDirectory() throws IOException {
    assertEquals("/path/dir,0,2000-01-01 00:00,1970-01-01 00:00" +
                ",0,0,0,700,1000,drwx-wx-w-+,user_1,group_1",
        createDelimitedWriterSpy().getEntry("/path/",
            createSampleDirInode()));
  }

  @Test
  public void testECXAttr() throws IOException {
    assertEquals("/path/dir,0,2000-01-01 00:00,1970-01-01 00:00" +
            ",0,0,0,700,1000,drwx-wx-w-+,user_1,group_1,-",
        createDelimitedWriterSpy(true).getEntry("/path/",
            createSampleDirInode(true)));
  }

  @Test
  public void testWriterOutputEntryBuilderForSymlink() throws IOException {
    assertEquals("/path/sym,0,2000-01-01 00:00,2000-01-01 00:00" +
                ",0,0,0,0,0,-rwx-wx-w-,user_1,group_1",
        createDelimitedWriterSpy().getEntry("/path/",
            createSampleSymlink()));
  }

  @Test
  public void testPBDelimitedWriter() throws IOException, InterruptedException {
    testPBDelimitedWriter("");  // Test in memory db.
    testPBDelimitedWriter(
        new FileSystemTestHelper().getTestRootDir() + "/delimited.db");
  }

  @Test
  public void testParallelPBDelimitedWriter() throws Exception {
    testParallelPBDelimitedWriter("");  // Test in memory db.
    testParallelPBDelimitedWriter(new FileSystemTestHelper().getTestRootDir()
        + "/parallel-delimited.db");
  }

  @Test
  public void testCorruptionOutputEntryBuilder() throws IOException {
    PBImageCorruptionDetector corrDetector =
        new PBImageCorruptionDetector(null, ",", "");
    PBImageCorruption c1 = new PBImageCorruption(342, true, false, 3);
    PBImageCorruptionDetector.OutputEntryBuilder entryBuilder1 =
        new PBImageCorruptionDetector.OutputEntryBuilder(corrDetector, false);
    entryBuilder1.setParentId(1)
        .setCorruption(c1)
        .setParentPath("/dir1/dir2/");
    assertEquals(entryBuilder1.build(),
        "MissingChild,342,false,/dir1/dir2/,1,,,3");

    corrDetector = new PBImageCorruptionDetector(null, "\t", "");
    PBImageCorruption c2 = new PBImageCorruption(781, false, true, 0);
    PBImageCorruptionDetector.OutputEntryBuilder entryBuilder2 =
        new PBImageCorruptionDetector.OutputEntryBuilder(corrDetector, true);
    entryBuilder2.setParentPath("/dir3/")
        .setCorruption(c2)
        .setName("folder")
        .setNodeType("Node");
    assertEquals(entryBuilder2.build(),
        "CorruptNode\t781\ttrue\t/dir3/\tMissing\tfolder\tNode\t0");
  }

  @Test
  public void testPBCorruptionDetector() throws IOException,
      InterruptedException {
    testPBCorruptionDetector("");  // Test in memory db.
    testPBCorruptionDetector(
        new FileSystemTestHelper().getTestRootDir() + "/corruption.db");
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

  @Test(expected = IOException.class)
  public void testDelimitedWithExistingFolder() throws IOException,
      InterruptedException {
    File tempDelimitedDir = null;
    try {
      String tempDelimitedDirName = "tempDirDelimited";
      String tempDelimitedDirPath = new FileSystemTestHelper().
          getTestRootDir() + "/" + tempDelimitedDirName;
      tempDelimitedDir = new File(tempDelimitedDirPath);
      Assert.assertTrue("Couldn't create temp directory!",
          tempDelimitedDir.mkdirs());
      testPBDelimitedWriter(tempDelimitedDirPath);
    } finally {
      if (tempDelimitedDir != null) {
        FileUtils.deleteDirectory(tempDelimitedDir);
      }
    }
  }

  private void testPBDelimitedWriter(String db)
      throws IOException, InterruptedException {
    final String DELIMITER = "\t";
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    try (PrintStream o = new PrintStream(output)) {
      PBImageDelimitedTextWriter v =
          new PBImageDelimitedTextWriter(o, DELIMITER, db);
      v.visit(originalFsimage.getAbsolutePath());
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
    for (Iterator<String> it = fileNames.iterator(); it.hasNext();) {
      String filename = it.next();
      if (filename.startsWith("/dirContainingInvalidXMLChar")) {
        it.remove();
      } else if (filename.equals("/")) {
        it.remove();
      }
    }
    assertEquals(writtenFiles.keySet(), fileNames);
  }

  private void testParallelPBDelimitedWriter(String db) throws Exception{
    String delimiter = "\t";
    int numThreads = 4;

    File parallelDelimitedOut = new File(tempDir, "parallelDelimitedOut");
    if (OfflineImageViewerPB.run(new String[] {"-p", "Delimited",
        "-i", originalFsimage.getAbsolutePath(),
        "-o", parallelDelimitedOut.getAbsolutePath(),
        "-delimiter", delimiter,
        "-t", db,
        "-m", String.valueOf(numThreads)}) != 0) {
      throw new IOException("oiv returned failure outputting in parallel.");
    }
    MD5Hash parallelMd5 = MD5FileUtils.computeMd5ForFile(parallelDelimitedOut);

    File serialDelimitedOut = new File(tempDir, "serialDelimitedOut");
    if (db != "") {
      db = db + "/../serial.db";
    }
    if (OfflineImageViewerPB.run(new String[] {"-p", "Delimited",
        "-i", originalFsimage.getAbsolutePath(),
        "-o", serialDelimitedOut.getAbsolutePath(),
        "-t", db,
        "-delimiter", delimiter}) != 0) {
      throw new IOException("oiv returned failure outputting in serial.");
    }
    MD5Hash serialMd5 = MD5FileUtils.computeMd5ForFile(serialDelimitedOut);

    assertEquals(parallelMd5, serialMd5);
  }

  private void testPBCorruptionDetector(String db)
      throws IOException, InterruptedException {
    final String delimiter = "\t";
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    try (PrintStream o = new PrintStream(output)) {
      PBImageCorruptionDetector v =
          new PBImageCorruptionDetector(o, delimiter, db);
      v.visit(originalFsimage.getAbsolutePath());
    }

    try (
        ByteArrayInputStream input =
            new ByteArrayInputStream(output.toByteArray());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(input))) {
      String line = reader.readLine();
      System.out.println(line);
      String[] fields = line.split(delimiter);
      assertEquals(8, fields.length);
      PBImageCorruptionDetector v =
          new PBImageCorruptionDetector(null, delimiter, "");
      assertEquals(line, v.getHeader());
      line = reader.readLine();
      assertNull(line);
    }
  }

  private void properINodeDelete(List<Long> idsToDelete, Document doc)
      throws IOException {
    NodeList inodes = doc.getElementsByTagName("id");
    if (inodes.getLength() < 1) {
      throw new IOException("No id tags found in the image xml.");
    }
    for (long idToDelete : idsToDelete) {
      boolean found = false;
      for (int i = 0; i < inodes.getLength(); i++) {
        Node id = inodes.item(i);
        if (id.getTextContent().equals(String.valueOf(idToDelete))) {
          found = true;
          Node inode = id.getParentNode();
          Node inodeSection = inode.getParentNode();
          inodeSection.removeChild(inode);
          break;
        }
      }
      if (!found) {
        throw new IOException("Couldn't find the id in the image.");
      }
    }
    NodeList numInodesNodes = doc.getElementsByTagName("numInodes");
    if (numInodesNodes.getLength() != 1) {
      throw new IOException("More than one numInodes tag found.");
    }
    Node numInodesNode = numInodesNodes.item(0);
    int numberOfINodes = Integer.parseInt(numInodesNode.getTextContent());
    numberOfINodes -= idsToDelete.size();
    numInodesNode.setTextContent(String.valueOf(numberOfINodes));
  }

  private void deleteINodeFromXML(File inputFile, File outputFile,
      List<Long> corruptibleIds) throws Exception {
    DocumentBuilderFactory docFactory = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
    Document doc = docBuilder.parse(inputFile);

    properINodeDelete(corruptibleIds, doc);

    TransformerFactory transformerFactory = XMLUtils.newSecureTransformerFactory();
    Transformer transformer = transformerFactory.newTransformer();
    DOMSource source = new DOMSource(doc);
    StreamResult result = new StreamResult(outputFile);
    transformer.transform(source, result);
  }

  private void generateMissingNodeCorruption(File goodImageXml,
      File corruptedImageXml, File corruptedImage, List<Long> corruptibleIds)
      throws Exception {
    if (OfflineImageViewerPB.run(new String[] {"-p", "XML",
        "-i", originalFsimage.getAbsolutePath(),
        "-o", goodImageXml.getAbsolutePath() }) != 0) {
      throw new IOException("Couldn't create XML!");
    }
    deleteINodeFromXML(goodImageXml, corruptedImageXml, corruptibleIds);
    if (OfflineImageViewerPB.run(new String[] {"-p", "ReverseXML",
        "-i", corruptedImageXml.getAbsolutePath(),
        "-o", corruptedImage.getAbsolutePath() }) != 0) {
      throw new IOException("Couldn't create from XML!");
    }
  }

  private String testCorruptionDetectorRun(int runNumber,
      List<Long> corruptions, String db) throws Exception {
    File goodImageXml = new File(tempDir, "goodImage" + runNumber +".xml");
    File corruptedImageXml = new File(tempDir,
        "corruptedImage" + runNumber + ".xml");
    File corruptedImage = new File(originalFsimage.getParent(),
        "fsimage_corrupted" + runNumber);
    generateMissingNodeCorruption(goodImageXml, corruptedImageXml,
        corruptedImage, corruptions);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (PrintStream o = new PrintStream(output)) {
      PBImageCorruptionDetector v =
          new PBImageCorruptionDetector(o, ",", db);
      v.visit(corruptedImage.getAbsolutePath());
    }
    return output.toString();
  }

  @Test
  public void testCorruptionDetectionSingleFileCorruption() throws Exception {
    List<Long> corruptions = Collections.singletonList(FILE_NODE_ID_1);
    String result = testCorruptionDetectorRun(1, corruptions, "");
    String expected = DFSTestUtil.readResoucePlainFile(
        "testSingleFileCorruption.csv");
    assertEquals(expected, result);
    result = testCorruptionDetectorRun(2, corruptions,
        new FileSystemTestHelper().getTestRootDir() + "/corruption2.db");
    assertEquals(expected, result);
  }

  @Test
  public void testCorruptionDetectionMultipleFileCorruption() throws Exception {
    List<Long> corruptions = Arrays.asList(FILE_NODE_ID_1, FILE_NODE_ID_2,
        FILE_NODE_ID_3);
    String result = testCorruptionDetectorRun(3, corruptions, "");
    String expected = DFSTestUtil.readResoucePlainFile(
        "testMultipleFileCorruption.csv");
    assertEquals(expected, result);
    result = testCorruptionDetectorRun(4, corruptions,
        new FileSystemTestHelper().getTestRootDir() + "/corruption4.db");
    assertEquals(expected, result);
  }

  @Test
  public void testCorruptionDetectionSingleFolderCorruption() throws Exception {
    List<Long> corruptions = Collections.singletonList(DIR_NODE_ID);
    String result = testCorruptionDetectorRun(5, corruptions, "");
    String expected = DFSTestUtil.readResoucePlainFile(
        "testSingleFolderCorruption.csv");
    assertEquals(expected, result);
    result = testCorruptionDetectorRun(6, corruptions,
        new FileSystemTestHelper().getTestRootDir() + "/corruption6.db");
    assertEquals(expected, result);
  }

  @Test
  public void testCorruptionDetectionMultipleCorruption() throws Exception {
    List<Long> corruptions = Arrays.asList(FILE_NODE_ID_1, FILE_NODE_ID_2,
        FILE_NODE_ID_3, DIR_NODE_ID);
    String result = testCorruptionDetectorRun(7, corruptions, "");
    String expected = DFSTestUtil.readResoucePlainFile(
        "testMultipleCorruption.csv");
    assertEquals(expected, result);
    result = testCorruptionDetectorRun(8, corruptions,
        new FileSystemTestHelper().getTestRootDir() + "/corruption8.db");
    assertEquals(expected, result);
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
    if (OfflineImageViewerPB.run(new String[] {"-p", "XML",
         "-i", originalFsimage.getAbsolutePath(),
         "-o", reverseImageXml.getAbsolutePath() }) != 0) {
      throw new IOException("oiv returned failure creating first XML file.");
    }
    if (OfflineImageViewerPB.run(new String[] {"-p", "ReverseXML",
          "-i", reverseImageXml.getAbsolutePath(),
          "-o", reverseImage.getAbsolutePath() }) != 0) {
      throw new IOException("oiv returned failure recreating fsimage file.");
    }
    if (OfflineImageViewerPB.run(new String[] {"-p", "XML",
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

  /**
   * Tests that the ReverseXML processor doesn't accept XML files without the SnapshotDiffSection.
   */
  @Test
  public void testReverseXmlWithoutSnapshotDiffSection() throws Throwable {
    File imageWSDS = new File(tempDir, "imageWithoutSnapshotDiffSection.xml");
    try(PrintWriter writer = new PrintWriter(imageWSDS, "UTF-8")) {
      writer.println("<?xml version=\"1.0\"?>");
      writer.println("<fsimage>");
      writer.println("<version>");
      writer.println("<layoutVersion>-67</layoutVersion>");
      writer.println("<onDiskVersion>1</onDiskVersion>");
      writer.println("<oivRevision>545bbef596c06af1c3c8dca1ce29096a64608478</oivRevision>");
      writer.println("</version>");
      writer.println("<FileUnderConstructionSection></FileUnderConstructionSection>");
      writer.println("<ErasureCodingSection></ErasureCodingSection>");
      writer.println("<INodeSection><lastInodeId>91488</lastInodeId><numInodes>0</numInodes>" +
              "</INodeSection>");
      writer.println("<SecretManagerSection><currentId>90</currentId><tokenSequenceNumber>35" +
              "</tokenSequenceNumber><numDelegationKeys>0</numDelegationKeys><numTokens>0" +
              "</numTokens></SecretManagerSection>");
      writer.println("<INodeReferenceSection></INodeReferenceSection>");
      writer.println("<SnapshotSection><snapshotCounter>0</snapshotCounter><numSnapshots>0" +
              "</numSnapshots></SnapshotSection>");
      writer.println("<NameSection><namespaceId>326384987</namespaceId></NameSection>");
      writer.println("<CacheManagerSection><nextDirectiveId>1</nextDirectiveId><numPools>0" +
              "</numPools><numDirectives>0</numDirectives></CacheManagerSection>");
      writer.println("<INodeDirectorySection></INodeDirectorySection>");
      writer.println("</fsimage>");
    }
      OfflineImageReconstructor.run(imageWSDS.getAbsolutePath(),
              imageWSDS.getAbsolutePath() + ".out");
  }

  @Test
  public void testFileDistributionCalculatorForException() throws Exception {
    File fsimageFile = null;
    Configuration conf = new Configuration();
    // Avoid using the same cluster dir to cause the global originalFsimage
    // file to be cleared.
    conf.set(HDFS_MINIDFS_BASEDIR, GenericTestUtils.getRandomizedTempPath());
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
      hdfs.setSafeMode(SafeModeAction.ENTER, false);
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

    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource(new StringReader(xml));
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

  /**
   * Tests that ReverseXML processor doesn't accept XML files with the CacheManagerSection contains pool or directive tag
   */
  @Test
  public void testReverseXmlWithCacheManagerSection() throws Throwable {
    File imageXml = new File(tempDir, "cacheManagerImageXml.xml");
    try (PrintWriter writer = new PrintWriter(imageXml, "UTF-8");) {
      writer.println("<?xml version=\"1.0\"?>");
      writer.println("<fsimage>");
      writer.println("<version>");
      writer.println(String.format("<layoutVersion>%d</layoutVersion>",
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
      writer.println("<onDiskVersion>1</onDiskVersion>");
      writer.println("<oivRevision>" +
          "545bbef596c06af1c3c8dca1ce29096a64608478</oivRevision>");
      writer.println("</version>");
      writer.println("<NameSection>"
          + "<namespaceId>1127691130</namespaceId>"
          + "<genstampV1>1000</genstampV1>"
          + "<genstampV2>1012</genstampV2>"
          + "<genstampV1Limit>0</genstampV1Limit>"
          + "<lastAllocatedBlockId>1073741836</lastAllocatedBlockId>"
          + "<txid>80</txid>"
          + "</NameSection>");
      writer.println("<INodeSection>"
          + "<lastInodeId>16405</lastInodeId>"
          + "<numInodes>1</numInodes>"
          + "<inode>"
          + "<id>16385</id>"
          + "<type>DIRECTORY</type>"
          + "<name></name>"
          + " <mtime>1458455831853</mtime>"
          + "<permission>zhexuan:supergroup:0755</permission>"
          + "<nsquota>9223372036854775807</nsquota>"
          + "<dsquota>-1</dsquota>"
          + "</inode>"+"</INodeSection>");
      writer.println("<INodeReferenceSection>"
          + "<ref>"
          + "<referredId>16404</referredId>" + "<name></name>"
          + "<dstSnapshotId>2147483646</dstSnapshotId>"
          + "<lastSnapshotId>0</lastSnapshotId>"
          + "</ref>"
          + "<ref>"
          + "<referredId>16404</referredId>"
          + "<name>orig</name>"
          + "<dstSnapshotId>0</dstSnapshotId>"
          + "<lastSnapshotId>0</lastSnapshotId>"
          + "</ref>"
          + "</INodeReferenceSection>");
      writer.println("<SnapshotSection>"
          + "<snapshotCounter>1</snapshotCounter>"
          + "<numSnapshots>1</numSnapshots>"
          + "<snapshottableDir>"
          + "<dir>16403</dir>"
          + "</snapshottableDir>"
          + "<snapshot>"
          + "<id>0</id>"
          + "<root>"
          + "<id>16403</id>"
          + "<type>DIRECTORY</type>"
          + "<name>snapshot</name>"
          + "<mtime>1458455831837</mtime>"
          + "<permission>zhexuan:supergroup:0755</permission>"
          + "<nsquota>-1</nsquota>"
          + "<dsquota>-1</dsquota>"
          + "</root>"
          + "</snapshot>"
          + "</SnapshotSection>");
      writer.println("<ErasureCodingSection></ErasureCodingSection>");
      writer.println("<INodeDirectorySection>"
          + "<directory>"
          + "<parent>16385</parent>"
          + "<child>16386</child>"
          + "<child>16391</child>"
          + "<child>16396</child>"
          + "<child>16402</child>"
          + "<child>16401</child>"
          + "<child>16403</child>"
          + "<child>16405</child>"
          + "<refChild>0</refChild>"
          + "</directory>" + "</INodeDirectorySection>");
      writer.println("<FileUnderConstructionSection></FileUnderConstructionSection>");
      writer.println("<SnapshotDiffSection>"
          + "<dirDiffEntry>"
          + "<inodeId>16385</inodeId>"
          + "<count>0</count>"
          + "</dirDiffEntry>" + "</SnapshotDiffSection>");
      writer.println("<SecretManagerSection>"
          + "<currentId>2</currentId>"
          + "<tokenSequenceNumber>1</tokenSequenceNumber>"
          + "<numDelegationKeys>2</numDelegationKeys>"
          + "<numTokens>1</numTokens>"
          + "<delegationKey>"
          + "<id>1</id>"
          + "<key>696c8a02c617790a</key>"
          + "<expiry>2016-03-20T06:37:19.806</expiry>"
          + "</delegationKey>"
          + "<delegationKey>"
          + "<id>2</id>"
          + "<key>11df597a666a6a2e</key>"
          + "<expiry>2016-03-21T06:37:19.807</expiry>"
          + "</delegationKey>"
          + "<token>"
          + "<owner>zhexuan</owner>"
          + "<renewer>JobTracker</renewer>"
          + "<realUser></realUser>"
          + "<issueDate>2016-03-20T06:37:11.810</issueDate>"
          + "<maxDate>2016-03-20T06:37:21.810</maxDate>"
          + "<sequenceNumber>1</sequenceNumber>"
          + "<masterKeyId>2</masterKeyId>"
          + "<expiryDate>2016-03-20T06:37:16.810</expiryDate>"
          + "</token>"
          + "</SecretManagerSection>");
      writer.println("<CacheManagerSection>");
      writer.println("<nextDirectiveId>1</nextDirectiveId>");
      writer.println("<numDirectives>0</numDirectives>");
      writer.println("<numPools>1</numPools>");
      writer.println("<pool>");
      writer.println("<poolName>0</poolName>");
      writer.println("<ownerName>1</ownerName>");
      writer.println("<groupName>2</groupName>");
      writer.println("<mode>3</mode>");
      writer.println("<limit>4</limit>");
      writer.println("<maxRelativeExpiry>455545555</maxRelativeExpiry>");
      writer.println("</pool>");
      writer.println("</CacheManagerSection>");
      writer.println("</fsimage>");
    }
    OfflineImageReconstructor.run(imageXml.getAbsolutePath(),
        imageXml.getAbsolutePath() + ".out");
  }
}
