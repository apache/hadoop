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
