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
package org.apache.hadoop.mapreduce.lib.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class TestFileInputFormat {

  @Test
  public void testNumInputFilesRecursively() throws Exception {
    Configuration conf = getConfiguration();
    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    Assert.assertEquals("Input splits are not correct", 3, splits.size());
    Assert.assertEquals("test:/a1/a2/file2", ((FileSplit) splits.get(0))
        .getPath().toString());
    Assert.assertEquals("test:/a1/a2/file3", ((FileSplit) splits.get(1))
        .getPath().toString());
    Assert.assertEquals("test:/a1/file1", ((FileSplit) splits.get(2)).getPath()
        .toString());
    
    // Using the deprecated configuration
    conf = getConfiguration();
    conf.set("mapred.input.dir.recursive", "true");
    job = Job.getInstance(conf);
    splits = fileInputFormat.getSplits(job);
    Assert.assertEquals("Input splits are not correct", 3, splits.size());
    Assert.assertEquals("test:/a1/a2/file2", ((FileSplit) splits.get(0))
        .getPath().toString());
    Assert.assertEquals("test:/a1/a2/file3", ((FileSplit) splits.get(1))
        .getPath().toString());
    Assert.assertEquals("test:/a1/file1", ((FileSplit) splits.get(2)).getPath()
        .toString());
  }

  @Test
  public void testNumInputFilesWithoutRecursively() throws Exception {
    Configuration conf = getConfiguration();
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    Assert.assertEquals("Input splits are not correct", 2, splits.size());
    Assert.assertEquals("test:/a1/a2", ((FileSplit) splits.get(0)).getPath()
        .toString());
    Assert.assertEquals("test:/a1/file1", ((FileSplit) splits.get(1)).getPath()
        .toString());
  }

  @Test
  public void testListLocatedStatus() throws Exception {
    Configuration conf = getConfiguration();
    conf.setBoolean("fs.test.impl.disable.cache", false);
    conf.set(FileInputFormat.INPUT_DIR, "test:///a1/a2");
    MockFileSystem mockFs =
        (MockFileSystem) new Path("test:///").getFileSystem(conf);
    Assert.assertEquals("listLocatedStatus already called",
        0, mockFs.numListLocatedStatusCalls);
    Job job = Job.getInstance(conf);
    FileInputFormat<?, ?> fileInputFormat = new TextInputFormat();
    List<InputSplit> splits = fileInputFormat.getSplits(job);
    Assert.assertEquals("Input splits are not correct", 2, splits.size());
    Assert.assertEquals("listLocatedStatuss calls",
        1, mockFs.numListLocatedStatusCalls);
  }

  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.test.impl.disable.cache", "true");
    conf.setClass("fs.test.impl", MockFileSystem.class, FileSystem.class);
    conf.set(FileInputFormat.INPUT_DIR, "test:///a1");
    return conf;
  }

  static class MockFileSystem extends RawLocalFileSystem {
    int numListLocatedStatusCalls = 0;

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
        IOException {
      if (f.toString().equals("test:/a1")) {
        return new FileStatus[] {
            new FileStatus(0, true, 1, 150, 150, new Path("test:/a1/a2")),
            new FileStatus(10, false, 1, 150, 150, new Path("test:/a1/file1")) };
      } else if (f.toString().equals("test:/a1/a2")) {
        return new FileStatus[] {
            new FileStatus(10, false, 1, 150, 150,
                new Path("test:/a1/a2/file2")),
            new FileStatus(10, false, 1, 151, 150,
                new Path("test:/a1/a2/file3")) };
      }
      return new FileStatus[0];
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
        throws IOException {
      return new FileStatus[] { new FileStatus(10, true, 1, 150, 150,
          pathPattern) };
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
        throws FileNotFoundException, IOException {
      return this.listStatus(f);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
        throws IOException {
      return new BlockLocation[] {
          new BlockLocation(new String[] { "localhost:50010" },
              new String[] { "localhost" }, 0, len) };
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f,
        PathFilter filter) throws FileNotFoundException, IOException {
      ++numListLocatedStatusCalls;
      return super.listLocatedStatus(f, filter);
    }
  }
}
