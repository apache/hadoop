/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Test case for FsUrlConnection with relativePath and SPACE.
 */
public class TestFsUrlConnectionPath {

  private static final String CURRENT = Paths.get("").toAbsolutePath()
      .toString();
  private static final String ABSOLUTE_PATH = "file:" + CURRENT + "/abs.txt";
  private static final String RELATIVE_PATH = "file:relative.txt";
  private static final String ABSOLUTE_PATH_W_SPACE = "file:"
      + CURRENT + "/abs 1.txt";
  private static final String RELATIVE_PATH_W_SPACE = "file:relative 1.txt";
  private static final String ABSOLUTE_PATH_W_ENCODED_SPACE =
      "file:" + CURRENT + "/abs%201.txt";
  private static final String RELATIVE_PATH_W_ENCODED_SPACE =
      "file:relative%201.txt";
  private static final String DATA = "data";
  private static final Configuration CONFIGURATION = new Configuration();


  @BeforeClass
  public static void initialize() throws IOException{
    write(ABSOLUTE_PATH.substring(5), DATA);
    write(RELATIVE_PATH.substring(5), DATA);
    write(ABSOLUTE_PATH_W_SPACE.substring(5), DATA);
    write(RELATIVE_PATH_W_SPACE.substring(5), DATA);
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
  }

  @AfterClass
  public static void cleanup(){
    delete(ABSOLUTE_PATH.substring(5));
    delete(RELATIVE_PATH.substring(5));
    delete(ABSOLUTE_PATH_W_SPACE.substring(5));
    delete(RELATIVE_PATH_W_SPACE.substring(5));
  }

  public static void delete(String path){
    File file = new File(path);
    file.delete();
  }

  public static void write(String path, String data) throws IOException{
    File file = new File(path);
    file.createNewFile();
    FileWriter fw = new FileWriter(file);
    fw.write(data);
    fw.close();
  }

  public static int readStream(String path) throws Exception{
    URL url = new URL(path);
    InputStream is = url.openStream();
    return is.available();
  }


  @Test
  public void testAbsolutePath() throws Exception{
    int length = readStream(ABSOLUTE_PATH);
    Assert.assertTrue(length > 1);
  }

  @Test
  public void testRelativePath() throws Exception{
    int length = readStream(RELATIVE_PATH);
    Assert.assertTrue(length > 1);
  }

  @Test
  public void testAbsolutePathWithSpace() throws Exception{
    int length = readStream(ABSOLUTE_PATH_W_ENCODED_SPACE);
    Assert.assertTrue(length > 1);
  }

  @Test
  public void testRelativePathWithSpace() throws Exception{
    int length = readStream(RELATIVE_PATH_W_ENCODED_SPACE);
    Assert.assertTrue(length > 1);
  }

}
