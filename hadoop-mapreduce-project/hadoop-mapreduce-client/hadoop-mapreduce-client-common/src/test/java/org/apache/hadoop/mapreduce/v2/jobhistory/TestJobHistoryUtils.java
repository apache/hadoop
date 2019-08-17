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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils.getConfiguredHistoryIntermediateUserDoneDirPermissions;


public class TestJobHistoryUtils {

  final static String TEST_DIR = new File(System.getProperty("test.build.data"))
        .getAbsolutePath();

  @Test
  @SuppressWarnings("unchecked")
  public void testGetHistoryDirsForCleaning() throws IOException {
    Path pRoot = new Path(TEST_DIR, "org.apache.hadoop.mapreduce.v2.jobhistory."
            + "TestJobHistoryUtils.testGetHistoryDirsForCleaning");
    FileContext fc = FileContext.getFileContext();
    Calendar cCal = Calendar.getInstance();
    int year = 2013;
    int month = 7;
    int day = 21;
    cCal.set(year, month - 1, day, 1, 0);
    long cutoff = cCal.getTimeInMillis();

    clearDir(fc, pRoot);
    Path pId00 = createPath(fc, pRoot, year, month, day, "000000");
    Path pId01 = createPath(fc, pRoot, year, month, day+1, "000001");
    Path pId02 = createPath(fc, pRoot, year, month, day-1, "000002");
    Path pId03 = createPath(fc, pRoot, year, month+1, day, "000003");
    Path pId04 = createPath(fc, pRoot, year, month+1, day+1, "000004");
    Path pId05 = createPath(fc, pRoot, year, month+1, day-1, "000005");
    Path pId06 = createPath(fc, pRoot, year, month-1, day, "000006");
    Path pId07 = createPath(fc, pRoot, year, month-1, day+1, "000007");
    Path pId08 = createPath(fc, pRoot, year, month-1, day-1, "000008");
    Path pId09 = createPath(fc, pRoot, year+1, month, day, "000009");
    Path pId10 = createPath(fc, pRoot, year+1, month, day+1, "000010");
    Path pId11 = createPath(fc, pRoot, year+1, month, day-1, "000011");
    Path pId12 = createPath(fc, pRoot, year+1, month+1, day, "000012");
    Path pId13 = createPath(fc, pRoot, year+1, month+1, day+1, "000013");
    Path pId14 = createPath(fc, pRoot, year+1, month+1, day-1, "000014");
    Path pId15 = createPath(fc, pRoot, year+1, month-1, day, "000015");
    Path pId16 = createPath(fc, pRoot, year+1, month-1, day+1, "000016");
    Path pId17 = createPath(fc, pRoot, year+1, month-1, day-1, "000017");
    Path pId18 = createPath(fc, pRoot, year-1, month, day, "000018");
    Path pId19 = createPath(fc, pRoot, year-1, month, day+1, "000019");
    Path pId20 = createPath(fc, pRoot, year-1, month, day-1, "000020");
    Path pId21 = createPath(fc, pRoot, year-1, month+1, day, "000021");
    Path pId22 = createPath(fc, pRoot, year-1, month+1, day+1, "000022");
    Path pId23 = createPath(fc, pRoot, year-1, month+1, day-1, "000023");
    Path pId24 = createPath(fc, pRoot, year-1, month-1, day, "000024");
    Path pId25 = createPath(fc, pRoot, year-1, month-1, day+1, "000025");
    Path pId26 = createPath(fc, pRoot, year-1, month-1, day-1, "000026");
    // non-expected names should be ignored without problems
    Path pId27 = createPath(fc, pRoot, "foo", "" + month, "" + day, "000027");
    Path pId28 = createPath(fc, pRoot, "" + year, "foo", "" + day, "000028");
    Path pId29 = createPath(fc, pRoot, "" + year, "" + month, "foo", "000029");

    List<FileStatus> dirs = JobHistoryUtils
       .getHistoryDirsForCleaning(fc, pRoot, cutoff);
    Collections.sort(dirs);
    Assert.assertEquals(14, dirs.size());
    Assert.assertEquals(pId26.toUri().getPath(),
        dirs.get(0).getPath().toUri().getPath());
    Assert.assertEquals(pId24.toUri().getPath(),
        dirs.get(1).getPath().toUri().getPath());
    Assert.assertEquals(pId25.toUri().getPath(),
        dirs.get(2).getPath().toUri().getPath());
    Assert.assertEquals(pId20.toUri().getPath(),
        dirs.get(3).getPath().toUri().getPath());
    Assert.assertEquals(pId18.toUri().getPath(),
        dirs.get(4).getPath().toUri().getPath());
    Assert.assertEquals(pId19.toUri().getPath(),
        dirs.get(5).getPath().toUri().getPath());
    Assert.assertEquals(pId23.toUri().getPath(),
        dirs.get(6).getPath().toUri().getPath());
    Assert.assertEquals(pId21.toUri().getPath(),
        dirs.get(7).getPath().toUri().getPath());
    Assert.assertEquals(pId22.toUri().getPath(),
        dirs.get(8).getPath().toUri().getPath());
    Assert.assertEquals(pId08.toUri().getPath(),
        dirs.get(9).getPath().toUri().getPath());
    Assert.assertEquals(pId06.toUri().getPath(),
        dirs.get(10).getPath().toUri().getPath());
    Assert.assertEquals(pId07.toUri().getPath(),
        dirs.get(11).getPath().toUri().getPath());
    Assert.assertEquals(pId02.toUri().getPath(),
        dirs.get(12).getPath().toUri().getPath());
    Assert.assertEquals(pId00.toUri().getPath(),
        dirs.get(13).getPath().toUri().getPath());
  }

  private void clearDir(FileContext fc, Path p) throws IOException {
    try {
      fc.delete(p, true);
    } catch (FileNotFoundException e) {
        // ignore
    }
    fc.mkdir(p, FsPermission.getDirDefault(), false);
  }

  private Path createPath(FileContext fc, Path root, int year, int month,
                          int day, String id) throws IOException {
    Path path = new Path(root, year + Path.SEPARATOR + month + Path.SEPARATOR +
            day + Path.SEPARATOR + id);
    fc.mkdir(path, FsPermission.getDirDefault(), true);
    return path;
  }

  private Path createPath(FileContext fc, Path root, String year, String month,
                          String day, String id) throws IOException {
    Path path = new Path(root, year + Path.SEPARATOR + month + Path.SEPARATOR +
            day + Path.SEPARATOR + id);
    fc.mkdir(path, FsPermission.getDirDefault(), true);
    return path;
  }

  @Test
  public void testGetConfiguredHistoryIntermediateUserDoneDirPermissions() {
    Configuration conf = new Configuration();
    Map<String, FsPermission> parameters = ImmutableMap.of(
      "775", new FsPermission(0775),
      "123", new FsPermission(0773),
      "-rwx", new FsPermission(0770) ,
      "+rwx", new FsPermission(0777)
    );
    for (Map.Entry<String, FsPermission> entry : parameters.entrySet()) {
      conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_USER_DONE_DIR_PERMISSIONS,
          entry.getKey());
      Assert.assertEquals(entry.getValue(),
          getConfiguredHistoryIntermediateUserDoneDirPermissions(conf));
    }
  }
}
