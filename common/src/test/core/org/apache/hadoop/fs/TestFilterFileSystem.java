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

package org.apache.hadoop.fs;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.EnumSet;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.util.Progressable;

public class TestFilterFileSystem extends TestCase {

  private static final Log LOG = FileSystem.LOG;

  public static class DontCheck {
    public BlockLocation[] getFileBlockLocations(Path p, 
        long start, long len) { return null; }
    public FsServerDefaults getServerDefaults() { return null; }
    public long getLength(Path f) { return 0; }
    public FSDataOutputStream append(Path f) { return null; }
    public FSDataOutputStream append(Path f, int bufferSize) { return null; }
    public void rename(final Path src, final Path dst, final Rename... options) { }
    public boolean exists(Path f) { return false; }
    public boolean isDirectory(Path f) { return false; }
    public boolean isFile(Path f) { return false; }
    public boolean createNewFile(Path f) { return false; }
    public boolean mkdirs(Path f) { return false; }
    public FSDataInputStream open(Path f) { return null; }
    public FSDataOutputStream create(Path f) { return null; }
    public FSDataOutputStream create(Path f, boolean overwrite) { return null; }
    public FSDataOutputStream create(Path f, Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f, short replication) {
      return null;
    }
    public FSDataOutputStream create(Path f, short replication, 
        Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f, 
        boolean overwrite,
        int bufferSize) {
      return null;
    }
    public FSDataOutputStream create(Path f, 
        boolean overwrite,
        int bufferSize,
        Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f, 
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize) {
      return null;
    }
    public FSDataOutputStream create(Path f,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) {
      return null;
    }
    public String getName() { return null; }
    public boolean delete(Path f) { return false; }
    public short getReplication(Path src) { return 0 ; }
    public void processDeleteOnExit() { }
    public ContentSummary getContentSummary(Path f) { return null; }
    public FsStatus getStatus() { return null; }
    public FileStatus[] listStatus(Path f, PathFilter filter) { return null; }
    public FileStatus[] listStatus(Path[] files) { return null; }
    public FileStatus[] listStatus(Path[] files, PathFilter filter) { return null; }
    public FileStatus[] globStatus(Path pathPattern) { return null; }
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) {
      return null;
    }
    public void copyFromLocalFile(Path src, Path dst) { }
    public void moveFromLocalFile(Path[] srcs, Path dst) { }
    public void moveFromLocalFile(Path src, Path dst) { }
    public void copyToLocalFile(Path src, Path dst) { }
    public void moveToLocalFile(Path src, Path dst) { }
    public long getBlockSize(Path f) { return 0; }
    public FSDataOutputStream primitiveCreate(final Path f,
        final EnumSet<CreateFlag> createFlag,
        CreateOpts... opts) { return null; }
    public void primitiveMkdir(Path f, FsPermission absolutePermission, 
                      boolean createParent) { }
  } 
  
  public void testFilterFileSystem() throws Exception {
    for (Method m : FileSystem.class.getDeclaredMethods()) {
      if (Modifier.isStatic(m.getModifiers()))
        continue;
      if (Modifier.isPrivate(m.getModifiers()))
        continue;
      
      try {
        DontCheck.class.getMethod(m.getName(), m.getParameterTypes());
        LOG.info("Skipping " + m);
      } catch (NoSuchMethodException exc) {
        LOG.info("Testing " + m);
        try{
          FilterFileSystem.class.getDeclaredMethod(m.getName(), m.getParameterTypes());
        }
        catch(NoSuchMethodException exc2){
          LOG.error("FilterFileSystem doesn't implement " + m);
          throw exc2;
        }
      }
    }
  }
  
}
