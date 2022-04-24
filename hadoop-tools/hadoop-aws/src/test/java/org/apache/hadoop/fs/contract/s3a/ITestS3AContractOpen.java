/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Ignore;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * S3A contract tests opening files.
 */
public class ITestS3AContractOpen extends AbstractContractOpenTest {
  private FSDataInputStream instream;

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * S3A always declares zero byte files as encrypted.
   * @return true, always.
   */
  @Override
  protected boolean areZeroByteFilesEncrypted() {
    return true;
  }

  /**
   * From HADOOP-17415, S3A will skip HEAD request to get file status when open file.
   * Therefore, FileNotFoundException will be delayed until the first read occur.
   */
  @Override
  public void testOpenReadDir() throws Throwable {
    describe("create & read a directory");
    Path path = path("zero.dir");
    mkdirs(path);

    try {
      instream = getFileSystem().open(path);
      int c = instream.read();
      fail("A directory has been opened for reading");
    } catch (FileNotFoundException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("opening a directory for reading", "FileNotFoundException", e);
    }
  }

  @Override
  public void testOpenReadDirWithChild() throws Throwable {
    describe("create & read a directory which has a child");
    Path path = path("zero.dir");
    mkdirs(path);
    Path path2 = new Path(path, "child");
    mkdirs(path2);

    try {
      instream = getFileSystem().open(path);
      int c = instream.read();
      fail("A directory has been opened for reading");
    } catch (FileNotFoundException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("opening a directory for reading", "FileNotFoundException", e);
    }
  }

  @Override
  public void testOpenFileLazyFail() throws Throwable {
    describe("openFile fails on a missing file in the read() and not before");
    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(path("testOpenFileLazyFail")).opt("fs.test.something", true);

    try {
      instream = builder.build().get();
      int c = instream.read();
      fail("A non existing file has been opened for reading");
    } catch (FileNotFoundException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("opening a non existing file for reading", "FileNotFoundException", e);
    }
  }

  @Override
  @Ignore
  public void testOpenFileFailExceptionally() throws Throwable {
    // does not fail on openFile
  }

  @Override
  @Ignore
  public void testAwaitFutureFailToFNFE() throws Throwable {
    // does not fail on openFile
  }

  @Override
  @Ignore
  public void testAwaitFutureTimeoutFailToFNFE() throws Throwable {
    // does not fail on openFile
  }

  @Override
  @Ignore
  public void testOpenFileExceptionallyTranslating() throws Throwable {
    // does not fail on openFile
  }

  @Override
  @Ignore
  public void testChainedFailureAwaitFuture() throws Throwable {
    // does not fail on openFile
  }
}
