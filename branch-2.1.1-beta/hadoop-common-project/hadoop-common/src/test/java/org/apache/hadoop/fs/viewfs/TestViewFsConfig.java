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
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.InodeTree;
import org.junit.Test;


public class TestViewFsConfig {
  
  
  @Test(expected=FileAlreadyExistsException.class)
  public void testInvalidConfig() throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2",
        new Path("file:///dir2").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2/linkToDir3",
        new Path("file:///dir3").toUri());
    
    class Foo { };
    
     new InodeTree<Foo>(conf, null) {

      @Override
      protected
      Foo getTargetFileSystem(final URI uri)
        throws URISyntaxException, UnsupportedFileSystemException {
          return null;
      }

      @Override
      protected
      Foo getTargetFileSystem(
          org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<Foo>
                                          dir)
        throws URISyntaxException {
        return null;
      }

      @Override
      protected
      Foo getTargetFileSystem(URI[] mergeFsURIList)
          throws URISyntaxException, UnsupportedFileSystemException {
        return null;
      }
    };
  }

}
