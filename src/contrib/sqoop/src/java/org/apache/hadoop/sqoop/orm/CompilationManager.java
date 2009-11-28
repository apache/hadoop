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

package org.apache.hadoop.sqoop.orm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.util.FileListing;

/**
 * Manages the compilation of a bunch of .java files into .class files
 * and eventually a jar.
 *
 * Also embeds this program's jar into the lib/ directory inside the compiled jar
 * to ensure that the job runs correctly.
 *
 * 
 *
 */
public class CompilationManager {

  public static final Log LOG = LogFactory.getLog(CompilationManager.class.getName());

  private ImportOptions options;
  private List<String> sources;

  public CompilationManager(final ImportOptions opts) {
    options = opts;
    sources = new ArrayList<String>();
  }

  public void addSourceFile(String sourceName) {
    sources.add(sourceName);
  }

  /**
   * locate the hadoop-*-core.jar in $HADOOP_HOME or --hadoop-home.
   * If that doesn't work, check our classpath.
   * @return the filename of the hadoop-*-core.jar file.
   */
  private String findHadoopCoreJar() {
    String hadoopHome = options.getHadoopHome();

    if (null == hadoopHome) {
      LOG.info("$HADOOP_HOME is not set");
      return findJarForClass(JobConf.class);
    }

    if (!hadoopHome.endsWith(File.separator)) {
      hadoopHome = hadoopHome + File.separator;
    }

    File hadoopHomeFile = new File(hadoopHome);
    LOG.info("HADOOP_HOME is " + hadoopHomeFile.getAbsolutePath());
    File [] entries = hadoopHomeFile.listFiles();

    if (null == entries) {
      LOG.warn("HADOOP_HOME appears empty or missing");
      return findJarForClass(JobConf.class);
    }

    for (File f : entries) {
      if (f.getName().startsWith("hadoop-") && f.getName().endsWith("-core.jar")) {
        LOG.info("Found hadoop core jar at: " + f.getAbsolutePath());
        return f.getAbsolutePath();
      }
    }

    return findJarForClass(JobConf.class);
  }

  /**
   * Compile the .java files into .class files via embedded javac call.
   */
  public void compile() throws IOException {
    List<String> args = new ArrayList<String>();

    // ensure that the jar output dir exists.
    String jarOutDir = options.getJarOutputDir();
    boolean mkdirSuccess = new File(jarOutDir).mkdirs();
    if (!mkdirSuccess) {
      LOG.debug("Warning: Could not make directories for " + jarOutDir);
    }

    // find hadoop-*-core.jar for classpath.
    String coreJar = findHadoopCoreJar();
    if (null == coreJar) {
      // Couldn't find a core jar to insert into the CP for compilation.
      // If, however, we're running this from a unit test, then the path
      // to the .class files might be set via the hadoop.alt.classpath property
      // instead. Check there first.
      String coreClassesPath = System.getProperty("hadoop.alt.classpath");
      if (null == coreClassesPath) {
        // no -- we're out of options. Fail.
        throw new IOException("Could not find hadoop core jar!");
      } else {
        coreJar = coreClassesPath;
      }
    }

    // find sqoop jar for compilation classpath
    String sqoopJar = findThisJar();
    if (null != sqoopJar) {
      sqoopJar = File.pathSeparator + sqoopJar;
    } else {
      LOG.warn("Could not find sqoop jar; child compilation may fail");
      sqoopJar = "";
    }

    String curClasspath = System.getProperty("java.class.path");

    args.add("-sourcepath");
    String srcOutDir = options.getCodeOutputDir();
    args.add(srcOutDir);

    args.add("-d");
    args.add(jarOutDir);

    args.add("-classpath");
    args.add(curClasspath + File.pathSeparator + coreJar + sqoopJar);

    // add all the source files
    for (String srcfile : sources) {
      args.add(srcOutDir + srcfile);
    }

    StringBuilder sb = new StringBuilder();
    for (String arg : args) {
      sb.append(arg + " ");
    }

    // NOTE(aaron): Usage is at http://java.sun.com/j2se/1.5.0/docs/tooldocs/solaris/javac.html
    LOG.debug("Invoking javac with args: " + sb.toString());
    int javacRet = com.sun.tools.javac.Main.compile(args.toArray(new String[0]));
    if (javacRet != 0) {
      throw new IOException("javac exited with status " + javacRet);
    }
  }

  /**
   * @return the complete filename of the .jar file to generate */
  public String getJarFilename() {
    String jarOutDir = options.getJarOutputDir();
    String tableName = options.getTableName();
    if (null != tableName && tableName.length() > 0) {
      return jarOutDir + tableName + ".jar";
    } else if (this.sources.size() == 1) {
      // if we only have one source file, find it's base name,
      // turn "foo.java" into "foo", and then return jarDir + "foo" + ".jar"
      String srcFileName = this.sources.get(0);
      String basename = new File(srcFileName).getName();
      String [] parts = basename.split("\\.");
      String preExtPart = parts[0];
      return jarOutDir + preExtPart + ".jar";
    } else {
      return jarOutDir + "sqoop.jar";
    }
  }

  /**
   * Searches through a directory and its children for .class
   * files to add to a jar.
   *
   * @param dir - The root directory to scan with this algorithm.
   * @param jstream - The JarOutputStream to write .class files to.
   */
  private void addClassFilesFromDir(File dir, JarOutputStream jstream)
      throws IOException {
    LOG.debug("Scanning for .class files in directory: " + dir);
    List<File> dirEntries = FileListing.getFileListing(dir);
    String baseDirName = dir.getAbsolutePath();
    if (!baseDirName.endsWith(File.separator)) {
      baseDirName = baseDirName + File.separator;
    }

    // for each input class file, create a zipfile entry for it,
    // read the file into a buffer, and write it to the jar file.
    for (File entry : dirEntries) {
      if (!entry.isDirectory()) {
        LOG.debug("Considering entry: " + entry);

        // chomp off the portion of the full path that is shared
        // with the base directory where class files were put;
        // we only record the subdir parts in the zip entry.
        String fullPath = entry.getAbsolutePath();
        String chompedPath = fullPath.substring(baseDirName.length());

        boolean include = chompedPath.endsWith(".class")
            && sources.contains(
            chompedPath.substring(0, chompedPath.length() - ".class".length()) + ".java");

        if (include) {
          // include this file.
          LOG.debug("Got classfile: " + entry.getPath() + " -> " + chompedPath);
          ZipEntry ze = new ZipEntry(chompedPath);
          jstream.putNextEntry(ze);
          copyFileToStream(entry, jstream);
          jstream.closeEntry();
        }
      }
    }
  }

  /**
   * Create an output jar file to use when executing MapReduce jobs
   */
  public void jar() throws IOException {
    String jarOutDir = options.getJarOutputDir();

    String jarFilename = getJarFilename();

    LOG.info("Writing jar file: " + jarFilename);

    File jarFileObj = new File(jarFilename);
    if (jarFileObj.exists()) {
      LOG.debug("Found existing jar (" + jarFilename + "); removing.");
      if (!jarFileObj.delete()) {
        LOG.warn("Could not remove existing jar file: " + jarFilename);
      }
    }

    FileOutputStream fstream = null;
    JarOutputStream jstream = null;
    try {
      fstream = new FileOutputStream(jarFilename);
      jstream = new JarOutputStream(fstream);

      addClassFilesFromDir(new File(jarOutDir), jstream);

      // put our own jar in there in its lib/ subdir
      String thisJarFile = findThisJar();
      if (null != thisJarFile) {
        File thisJarFileObj = new File(thisJarFile);
        String thisJarBasename = thisJarFileObj.getName();
        String thisJarEntryName = "lib" + File.separator + thisJarBasename;
        ZipEntry ze = new ZipEntry(thisJarEntryName);
        jstream.putNextEntry(ze);
        copyFileToStream(thisJarFileObj, jstream);
        jstream.closeEntry();
      } else {
        // couldn't find our own jar (we were running from .class files?)
        LOG.warn("Could not find jar for Sqoop; MapReduce jobs may not run correctly.");
      }

      jstream.finish();
    } finally {
      if (null != jstream) {
        try {
          jstream.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing jar stream: " + ioe.toString());
        }
      }

      if (null != fstream) {
        try {
          fstream.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing file stream: " + ioe.toString());
        }
      }
    }

    LOG.debug("Finished writing jar file " + jarFilename);
  }


  private static final int BUFFER_SZ = 4096;

  /**
   * utility method to copy a .class file into the jar stream.
   * @param f
   * @param ostream
   * @throws IOException
   */
  private void copyFileToStream(File f, OutputStream ostream) throws IOException {
    FileInputStream fis = new FileInputStream(f);
    byte [] buffer = new byte[BUFFER_SZ];
    try {
      while (true) {
        int bytesReceived = fis.read(buffer);
        if (bytesReceived < 1) {
          break;
        }

        ostream.write(buffer, 0, bytesReceived);
      }
    } finally {
      fis.close();
    }
  }

  private String findThisJar() {
    return findJarForClass(CompilationManager.class);
  }

  // method mostly cloned from o.a.h.mapred.JobConf.findContainingJar()
  private String findJarForClass(Class<? extends Object> classObj) {
    ClassLoader loader = classObj.getClassLoader();
    String classFile = classObj.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<URL> itr = loader.getResources(classFile);
          itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          // URLDecoder is a misnamed class, since it actually decodes
          // x-www-form-urlencoded MIME type rather than actual
          // URL encoding (which the file path has). Therefore it would
          // decode +s to ' 's which is incorrect (spaces are actually
          // either unencoded or encoded as "%20"). Replace +s first, so
          // that they are kept sacred during the decoding process.
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
