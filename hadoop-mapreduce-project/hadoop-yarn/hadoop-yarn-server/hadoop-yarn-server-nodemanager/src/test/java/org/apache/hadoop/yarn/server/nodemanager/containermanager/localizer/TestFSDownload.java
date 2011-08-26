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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.FSDownload;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static org.apache.hadoop.fs.CreateFlag.*;


import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestFSDownload {

  @AfterClass
  public static void deleteTestDir() throws IOException {
    FileContext fs = FileContext.getLocalFSFileContext();
    fs.delete(new Path("target", TestFSDownload.class.getSimpleName()), true);
  }

  static final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  static LocalResource createFile(FileContext files, Path p, int len,
      Random r) throws IOException, URISyntaxException {
    FSDataOutputStream out = null;
    try {
      byte[] bytes = new byte[len];
      out = files.create(p, EnumSet.of(CREATE, OVERWRITE));
      r.nextBytes(bytes);
      out.write(bytes);
    } finally {
      if (out != null) out.close();
    }
    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    ret.setResource(ConverterUtils.getYarnUrlFromPath(p));
    ret.setSize(len);
    ret.setType(LocalResourceType.FILE);
    ret.setTimestamp(files.getFileStatus(p).getModificationTime());
    return ret;
  }

  @Test
  public void testDownload() throws IOException, URISyntaxException,
      InterruptedException {
    Configuration conf = new Configuration();
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(new Path("target",
      TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());

    Random rand = new Random();
    long sharedSeed = rand.nextLong();
    rand.setSeed(sharedSeed);
    System.out.println("SEED: " + sharedSeed);

    Map<LocalResource,Future<Path>> pending =
      new HashMap<LocalResource,Future<Path>>();
    ExecutorService exec = Executors.newSingleThreadExecutor();
    LocalDirAllocator dirs =
      new LocalDirAllocator(TestFSDownload.class.getName());
    int[] sizes = new int[10];
    for (int i = 0; i < 10; ++i) {
      sizes[i] = rand.nextInt(512) + 512;
      LocalResource rsrc = createFile(files, new Path(basedir, "" + i),
          sizes[i], rand);
      FSDownload fsd =
          new FSDownload(files, UserGroupInformation.getCurrentUser(), conf,
              dirs, rsrc, new Random(sharedSeed));
      pending.put(rsrc, exec.submit(fsd));
    }

    try {
      for (Map.Entry<LocalResource,Future<Path>> p : pending.entrySet()) {
        Path localized = p.getValue().get();
        assertEquals(sizes[Integer.valueOf(localized.getName())], p.getKey()
            .getSize());
      }
    } catch (ExecutionException e) {
      throw new IOException("Failed exec", e);
    } finally {
      exec.shutdown();
    }
  }

}
