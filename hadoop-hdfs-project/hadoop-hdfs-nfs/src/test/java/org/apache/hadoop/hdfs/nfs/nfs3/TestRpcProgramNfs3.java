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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.jboss.netty.channel.Channel;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.LOOKUP3Request;
import org.apache.hadoop.nfs.nfs3.request.READ3Request;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.ACCESS3Response;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.nfs.nfs3.response.CREATE3Response;
import org.apache.hadoop.nfs.nfs3.response.FSSTAT3Response;
import org.apache.hadoop.nfs.nfs3.response.FSINFO3Response;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.LOOKUP3Response;
import org.apache.hadoop.nfs.nfs3.response.PATHCONF3Response;
import org.apache.hadoop.nfs.nfs3.response.READ3Response;
import org.apache.hadoop.nfs.nfs3.response.REMOVE3Response;
import org.apache.hadoop.nfs.nfs3.response.RMDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.RENAME3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIRPLUS3Response;
import org.apache.hadoop.nfs.nfs3.response.READLINK3Response;
import org.apache.hadoop.nfs.nfs3.response.SETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.SYMLINK3Response;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.SecurityHandler;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;


/**
 * Tests for {@link RpcProgramNfs3}
 */
public class TestRpcProgramNfs3 {
  static DistributedFileSystem hdfs;
  static MiniDFSCluster cluster = null;
  static NfsConfiguration config = new NfsConfiguration();
  static NameNode nn;
  static Nfs3 nfs;
  static RpcProgramNfs3 nfsd;
  static SecurityHandler securityHandler;
  static SecurityHandler securityHandlerUnpriviledged;
  static String testdir = "/tmp";

  @BeforeClass
  public static void setup() throws Exception {
    String currentUser = System.getProperty("user.name");

    config.set("fs.permissions.umask-mode", "u=rwx,g=,o=");
    config.set(DefaultImpersonationProvider.getTestProvider()
        .getProxySuperuserGroupConfKey(currentUser), "*");
    config.set(DefaultImpersonationProvider.getTestProvider()
        .getProxySuperuserIpConfKey(currentUser), "*");
    ProxyUsers.refreshSuperUserGroupsConfiguration(config);

    cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
    nn = cluster.getNameNode();

    // Use ephemeral ports in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);

    // Start NFS with allowed.hosts set to "* rw"
    config.set("dfs.nfs.exports.allowed.hosts", "* rw");
    nfs = new Nfs3(config);
    nfs.startServiceInternal(false);
    nfsd = (RpcProgramNfs3) nfs.getRpcProgram();


    // Mock SecurityHandler which returns system user.name
    securityHandler = Mockito.mock(SecurityHandler.class);
    Mockito.when(securityHandler.getUser()).thenReturn(currentUser);

    // Mock SecurityHandler which returns a dummy username "harry"
    securityHandlerUnpriviledged = Mockito.mock(SecurityHandler.class);
    Mockito.when(securityHandlerUnpriviledged.getUser()).thenReturn("harry");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void createFiles() throws IllegalArgumentException, IOException {
    hdfs.delete(new Path(testdir), true);
    hdfs.mkdirs(new Path(testdir));
    hdfs.mkdirs(new Path(testdir + "/foo"));
    DFSTestUtil.createFile(hdfs, new Path(testdir + "/bar"), 0, (short) 1, 0);
  }

  @Test(timeout = 60000)
  public void testGetattr() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    GETATTR3Response response1 = nfsd.getattr(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    GETATTR3Response response2 = nfsd.getattr(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testSetattr() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("bar");
    SetAttr3 symAttr = new SetAttr3();
    symAttr.serialize(xdr_req);
    xdr_req.writeBoolean(false);

    // Attempt by an unpriviledged user should fail.
    SETATTR3Response response1 = nfsd.setattr(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    SETATTR3Response response2 = nfsd.setattr(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testLookup() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    LOOKUP3Request lookupReq = new LOOKUP3Request(handle, "bar");
    XDR xdr_req = new XDR();
    lookupReq.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    LOOKUP3Response response1 = nfsd.lookup(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    LOOKUP3Response response2 = nfsd.lookup(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testAccess() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    ACCESS3Response response1 = nfsd.access(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    ACCESS3Response response2 = nfsd.access(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testReadlink() throws Exception {
    // Create a symlink first.
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("fubar");
    SetAttr3 symAttr = new SetAttr3();
    symAttr.serialize(xdr_req);
    xdr_req.writeString("bar");

    SYMLINK3Response response = nfsd.symlink(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response.getStatus());

    // Now perform readlink operations.
    FileHandle handle2 = response.getObjFileHandle();
    XDR xdr_req2 = new XDR();
    handle2.serialize(xdr_req2);

    // Attempt by an unpriviledged user should fail.
    READLINK3Response response1 = nfsd.readlink(xdr_req2.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    READLINK3Response response2 = nfsd.readlink(xdr_req2.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testRead() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);

    READ3Request readReq = new READ3Request(handle, 0, 5);
    XDR xdr_req = new XDR();
    readReq.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    READ3Response response1 = nfsd.read(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    READ3Response response2 = nfsd.read(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testWrite() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);

    byte[] buffer = new byte[10];
    for (int i = 0; i < 10; i++) {
      buffer[i] = (byte) i;
    }

    WRITE3Request writeReq = new WRITE3Request(handle, 0, 10,
        WriteStableHow.DATA_SYNC, ByteBuffer.wrap(buffer));
    XDR xdr_req = new XDR();
    writeReq.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    WRITE3Response response1 = nfsd.write(xdr_req.asReadOnlyWrap(),
        null, 1, securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    WRITE3Response response2 = nfsd.write(xdr_req.asReadOnlyWrap(),
        null, 1, securityHandler,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect response:", null, response2);
  }

  @Test(timeout = 60000)
  public void testCreate() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("fubar");
    xdr_req.writeInt(Nfs3Constant.CREATE_UNCHECKED);
    SetAttr3 symAttr = new SetAttr3();
    symAttr.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    CREATE3Response response1 = nfsd.create(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    CREATE3Response response2 = nfsd.create(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testMkdir() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("fubar");
    SetAttr3 symAttr = new SetAttr3();
    symAttr.serialize(xdr_req);
    xdr_req.writeString("bar");

    // Attempt to remove by an unpriviledged user should fail.
    SYMLINK3Response response1 = nfsd.symlink(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt to remove by a priviledged user should pass.
    SYMLINK3Response response2 = nfsd.symlink(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testSymlink() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("fubar");
    SetAttr3 symAttr = new SetAttr3();
    symAttr.serialize(xdr_req);
    xdr_req.writeString("bar");

    // Attempt by an unpriviledged user should fail.
    SYMLINK3Response response1 = nfsd.symlink(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    SYMLINK3Response response2 = nfsd.symlink(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testRemove() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("bar");

    // Attempt by an unpriviledged user should fail.
    REMOVE3Response response1 = nfsd.remove(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    REMOVE3Response response2 = nfsd.remove(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testRmdir() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("foo");

    // Attempt by an unpriviledged user should fail.
    RMDIR3Response response1 = nfsd.rmdir(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    RMDIR3Response response2 = nfsd.rmdir(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testRename() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    XDR xdr_req = new XDR();
    FileHandle handle = new FileHandle(dirId);
    handle.serialize(xdr_req);
    xdr_req.writeString("bar");
    handle.serialize(xdr_req);
    xdr_req.writeString("fubar");

    // Attempt by an unpriviledged user should fail.
    RENAME3Response response1 = nfsd.rename(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    RENAME3Response response2 = nfsd.rename(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testReaddir() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);
    xdr_req.writeLongAsHyper(0);
    xdr_req.writeLongAsHyper(0);
    xdr_req.writeInt(100);

    // Attempt by an unpriviledged user should fail.
    READDIR3Response response1 = nfsd.readdir(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    READDIR3Response response2 = nfsd.readdir(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testReaddirplus() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo(testdir);
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);
    xdr_req.writeLongAsHyper(0);
    xdr_req.writeLongAsHyper(0);
    xdr_req.writeInt(3);
    xdr_req.writeInt(2);

    // Attempt by an unpriviledged user should fail.
    READDIRPLUS3Response response1 = nfsd.readdirplus(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    READDIRPLUS3Response response2 = nfsd.readdirplus(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testFsstat() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    FSSTAT3Response response1 = nfsd.fsstat(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    FSSTAT3Response response2 = nfsd.fsstat(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testFsinfo() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    FSINFO3Response response1 = nfsd.fsinfo(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    FSINFO3Response response2 = nfsd.fsinfo(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testPathconf() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);

    // Attempt by an unpriviledged user should fail.
    PATHCONF3Response response1 = nfsd.pathconf(xdr_req.asReadOnlyWrap(),
        securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    PATHCONF3Response response2 = nfsd.pathconf(xdr_req.asReadOnlyWrap(),
        securityHandler, new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3_OK,
        response2.getStatus());
  }

  @Test(timeout = 60000)
  public void testCommit() throws Exception {
    HdfsFileStatus status = nn.getRpcServer().getFileInfo("/tmp/bar");
    long dirId = status.getFileId();
    FileHandle handle = new FileHandle(dirId);
    XDR xdr_req = new XDR();
    handle.serialize(xdr_req);
    xdr_req.writeLongAsHyper(0);
    xdr_req.writeInt(5);

    Channel ch = Mockito.mock(Channel.class);

    // Attempt by an unpriviledged user should fail.
    COMMIT3Response response1 = nfsd.commit(xdr_req.asReadOnlyWrap(),
        ch, 1, securityHandlerUnpriviledged,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect return code:", Nfs3Status.NFS3ERR_ACCES,
        response1.getStatus());

    // Attempt by a priviledged user should pass.
    COMMIT3Response response2 = nfsd.commit(xdr_req.asReadOnlyWrap(),
        ch, 1, securityHandler,
        new InetSocketAddress("localhost", 1234));
    assertEquals("Incorrect COMMIT3Response:", null, response2);
  }

  @Test(timeout=1000)
  public void testIdempotent() {
    Object[][] procedures = {
        { Nfs3Constant.NFSPROC3.NULL, 1 },
        { Nfs3Constant.NFSPROC3.GETATTR, 1 },
        { Nfs3Constant.NFSPROC3.SETATTR, 1 },
        { Nfs3Constant.NFSPROC3.LOOKUP, 1 },
        { Nfs3Constant.NFSPROC3.ACCESS, 1 },
        { Nfs3Constant.NFSPROC3.READLINK, 1 },
        { Nfs3Constant.NFSPROC3.READ, 1 },
        { Nfs3Constant.NFSPROC3.WRITE, 1 },
        { Nfs3Constant.NFSPROC3.CREATE, 0 },
        { Nfs3Constant.NFSPROC3.MKDIR, 0 },
        { Nfs3Constant.NFSPROC3.SYMLINK, 0 },
        { Nfs3Constant.NFSPROC3.MKNOD, 0 },
        { Nfs3Constant.NFSPROC3.REMOVE, 0 },
        { Nfs3Constant.NFSPROC3.RMDIR, 0 },
        { Nfs3Constant.NFSPROC3.RENAME, 0 },
        { Nfs3Constant.NFSPROC3.LINK, 0 },
        { Nfs3Constant.NFSPROC3.READDIR, 1 },
        { Nfs3Constant.NFSPROC3.READDIRPLUS, 1 },
        { Nfs3Constant.NFSPROC3.FSSTAT, 1 },
        { Nfs3Constant.NFSPROC3.FSINFO, 1 },
        { Nfs3Constant.NFSPROC3.PATHCONF, 1 },
        { Nfs3Constant.NFSPROC3.COMMIT, 1 } };
    for (Object[] procedure : procedures) {
      boolean idempotent = procedure[1].equals(Integer.valueOf(1));
      Nfs3Constant.NFSPROC3 proc = (Nfs3Constant.NFSPROC3)procedure[0];
      if (idempotent) {
        Assert.assertTrue(("Procedure " + proc + " should be idempotent"),
            proc.isIdempotent());
      } else {
        Assert.assertFalse(("Procedure " + proc + " should be non-idempotent"),
            proc.isIdempotent());
      }
    }
  }

  @Test
  public void testDeprecatedKeys() {
    NfsConfiguration conf = new NfsConfiguration();
    conf.setInt("nfs3.server.port", 998);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_SERVER_PORT_KEY, 0) == 998);

    conf.setInt("nfs3.mountd.port", 999);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_MOUNTD_PORT_KEY, 0) == 999);

    conf.set("dfs.nfs.exports.allowed.hosts", "host1");
    assertTrue(conf.get(CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_KEY)
        .equals("host1"));

    conf.setInt("dfs.nfs.exports.cache.expirytime.millis", 1000);
    assertTrue(conf.getInt(
        Nfs3Constant.NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_KEY, 0) == 1000);

    conf.setInt("hadoop.nfs.userupdate.milly", 10);
    assertTrue(conf.getInt(Nfs3Constant.NFS_USERGROUP_UPDATE_MILLIS_KEY, 0) == 10);

    conf.set("dfs.nfs3.dump.dir", "/nfs/tmp");
    assertTrue(conf.get(NfsConfigKeys.DFS_NFS_FILE_DUMP_DIR_KEY).equals(
        "/nfs/tmp"));

    conf.setBoolean("dfs.nfs3.enableDump", false);
    assertTrue(conf.getBoolean(NfsConfigKeys.DFS_NFS_FILE_DUMP_KEY, true) == false);

    conf.setInt("dfs.nfs3.max.open.files", 500);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_MAX_OPEN_FILES_KEY, 0) == 500);

    conf.setInt("dfs.nfs3.stream.timeout", 6000);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_KEY, 0) == 6000);

    conf.set("dfs.nfs3.export.point", "/dir1");
    assertTrue(conf.get(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY).equals("/dir1"));
  }
}
