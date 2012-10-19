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

package org.apache.hadoop.lib.service.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.FileSystemAccess;
import org.apache.hadoop.lib.service.FileSystemAccessException;
import org.apache.hadoop.lib.service.instrumentation.InstrumentationService;
import org.apache.hadoop.lib.service.scheduler.SchedulerService;
import org.apache.hadoop.test.HFSTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestException;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestHdfsHelper;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFileSystemAccessService extends HFSTestCase {

  private void createHadoopConf(Configuration hadoopConf) throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    File hdfsSite = new File(dir, "hdfs-site.xml");
    OutputStream os = new FileOutputStream(hdfsSite);
    hadoopConf.writeXml(os);
    os.close();
  }

  @Before
  public void createHadoopConf() throws Exception {
    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set("foo", "FOO");
    createHadoopConf(hadoopConf);
  }

  @Test
  @TestDir
  public void simpleSecurity() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    Assert.assertNotNull(server.get(FileSystemAccess.class));
    server.destroy();
  }

  @Test
  @TestException(exception = ServiceException.class, msgRegExp = "H01.*")
  @TestDir
  public void noKerberosKeytabProperty() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
    Arrays.asList(InstrumentationService.class.getName(),
                  SchedulerService.class.getName(),
                  FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.authentication.type", "kerberos");
    conf.set("server.hadoop.authentication.kerberos.keytab", " ");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServiceException.class, msgRegExp = "H01.*")
  @TestDir
  public void noKerberosPrincipalProperty() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.authentication.type", "kerberos");
    conf.set("server.hadoop.authentication.kerberos.keytab", "/tmp/foo");
    conf.set("server.hadoop.authentication.kerberos.principal", " ");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServiceException.class, msgRegExp = "H02.*")
  @TestDir
  public void kerberosInitializationFailure() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.authentication.type", "kerberos");
    conf.set("server.hadoop.authentication.kerberos.keytab", "/tmp/foo");
    conf.set("server.hadoop.authentication.kerberos.principal", "foo@FOO");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServiceException.class, msgRegExp = "H09.*")
  @TestDir
  public void invalidSecurity() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.authentication.type", "foo");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestDir
  public void serviceHadoopConf() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);

    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccessService fsAccess = (FileSystemAccessService) server.get(FileSystemAccess.class);
    Assert.assertEquals(fsAccess.serviceHadoopConf.get("foo"), "FOO");
    server.destroy();
  }

  @Test
  @TestDir
  public void serviceHadoopConfCustomDir() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String hadoopConfDir = new File(dir, "confx").getAbsolutePath();
    new File(hadoopConfDir).mkdirs();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.config.dir", hadoopConfDir);

    File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
    OutputStream os = new FileOutputStream(hdfsSite);
    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set("foo", "BAR");
    hadoopConf.writeXml(os);
    os.close();

    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccessService fsAccess = (FileSystemAccessService) server.get(FileSystemAccess.class);
    Assert.assertEquals(fsAccess.serviceHadoopConf.get("foo"), "BAR");
    server.destroy();
  }

  @Test
  @TestDir
  public void inWhitelists() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccessService fsAccess = (FileSystemAccessService) server.get(FileSystemAccess.class);
    fsAccess.validateNamenode("NN");
    server.destroy();

    conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.name.node.whitelist", "*");
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    fsAccess = (FileSystemAccessService) server.get(FileSystemAccess.class);
    fsAccess.validateNamenode("NN");
    server.destroy();

    conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.name.node.whitelist", "NN");
    server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    fsAccess = (FileSystemAccessService) server.get(FileSystemAccess.class);
    fsAccess.validateNamenode("NN");
    server.destroy();
  }

  @Test
  @TestException(exception = FileSystemAccessException.class, msgRegExp = "H05.*")
  @TestDir
  public void NameNodeNotinWhitelists() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.name.node.whitelist", "NN");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccessService fsAccess = (FileSystemAccessService) server.get(FileSystemAccess.class);
    fsAccess.validateNamenode("NNx");
  }

  @Test
  @TestDir
  @TestHdfs
  public void createFileSystem() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));

    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, TestHdfsHelper.getHdfsConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    createHadoopConf(hadoopConf);

    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.filesystem.cache.purge.timeout", "0");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccess hadoop = server.get(FileSystemAccess.class);
    FileSystem fs = hadoop.createFileSystem("u", hadoop.getFileSystemConfiguration());
    Assert.assertNotNull(fs);
    fs.mkdirs(new Path("/tmp/foo"));
    hadoop.releaseFileSystem(fs);
    try {
      fs.mkdirs(new Path("/tmp/foo"));
      Assert.fail();
    } catch (IOException ex) {
    } catch (Exception ex) {
      Assert.fail();
    }
    server.destroy();
  }

  @Test
  @TestDir
  @TestHdfs
  public void fileSystemExecutor() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));

    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, TestHdfsHelper.getHdfsConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    createHadoopConf(hadoopConf);

    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.filesystem.cache.purge.timeout", "0");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccess hadoop = server.get(FileSystemAccess.class);

    final FileSystem fsa[] = new FileSystem[1];

    hadoop.execute("u", hadoop.getFileSystemConfiguration(), new FileSystemAccess.FileSystemExecutor<Void>() {
      @Override
      public Void execute(FileSystem fs) throws IOException {
        fs.mkdirs(new Path("/tmp/foo"));
        fsa[0] = fs;
        return null;
      }
    });
    try {
      fsa[0].mkdirs(new Path("/tmp/foo"));
      Assert.fail();
    } catch (IOException ex) {
    } catch (Exception ex) {
      Assert.fail();
    }
    server.destroy();
  }

  @Test
  @TestException(exception = FileSystemAccessException.class, msgRegExp = "H06.*")
  @TestDir
  @TestHdfs
  public void fileSystemExecutorNoNameNode() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));
    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, TestHdfsHelper.getHdfsConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    createHadoopConf(hadoopConf);

    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccess fsAccess = server.get(FileSystemAccess.class);

    Configuration hdfsConf = fsAccess.getFileSystemConfiguration();
    hdfsConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "");
    fsAccess.execute("u", hdfsConf, new FileSystemAccess.FileSystemExecutor<Void>() {
      @Override
      public Void execute(FileSystem fs) throws IOException {
        return null;
      }
    });
  }

  @Test
  @TestDir
  @TestHdfs
  public void fileSystemExecutorException() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));

    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, TestHdfsHelper.getHdfsConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    createHadoopConf(hadoopConf);

    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.filesystem.cache.purge.timeout", "0");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    FileSystemAccess hadoop = server.get(FileSystemAccess.class);

    final FileSystem fsa[] = new FileSystem[1];
    try {
      hadoop.execute("u", hadoop.getFileSystemConfiguration(), new FileSystemAccess.FileSystemExecutor<Void>() {
        @Override
        public Void execute(FileSystem fs) throws IOException {
          fsa[0] = fs;
          throw new IOException();
        }
      });
      Assert.fail();
    } catch (FileSystemAccessException ex) {
      Assert.assertEquals(ex.getError(), FileSystemAccessException.ERROR.H03);
    } catch (Exception ex) {
      Assert.fail();
    }

    try {
      fsa[0].mkdirs(new Path("/tmp/foo"));
      Assert.fail();
    } catch (IOException ex) {
    } catch (Exception ex) {
      Assert.fail();
    }
    server.destroy();
  }

  @Test
  @TestDir
  @TestHdfs
  public void fileSystemCache() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    String services = StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
                    SchedulerService.class.getName(),
                    FileSystemAccessService.class.getName()));

    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
      TestHdfsHelper.getHdfsConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    createHadoopConf(hadoopConf);

    Configuration conf = new Configuration(false);
    conf.set("server.services", services);
    conf.set("server.hadoop.filesystem.cache.purge.frequency", "1");
    conf.set("server.hadoop.filesystem.cache.purge.timeout", "1");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    try {
      server.init();
      FileSystemAccess hadoop = server.get(FileSystemAccess.class);

      FileSystem fs1 =
        hadoop.createFileSystem("u", hadoop.getFileSystemConfiguration());
      Assert.assertNotNull(fs1);
      fs1.mkdirs(new Path("/tmp/foo1"));
      hadoop.releaseFileSystem(fs1);

      //still around because of caching
      fs1.mkdirs(new Path("/tmp/foo2"));

      FileSystem fs2 =
        hadoop.createFileSystem("u", hadoop.getFileSystemConfiguration());

      //should be same instance because of caching
      Assert.assertEquals(fs1, fs2);

      Thread.sleep(4 * 1000);

      //still around because of lease count is 1 (fs2 is out)
      fs1.mkdirs(new Path("/tmp/foo2"));

      Thread.sleep(4 * 1000);

      //still around because of lease count is 1 (fs2 is out)
      fs2.mkdirs(new Path("/tmp/foo"));

      hadoop.releaseFileSystem(fs2);
      Thread.sleep(4 * 1000);

      //should not be around as lease count is 0
      try {
        fs2.mkdirs(new Path("/tmp/foo"));
        Assert.fail();
      } catch (IOException ex) {
      } catch (Exception ex) {
        Assert.fail();
      }
    } finally {
      server.destroy();
    }
  }

}
