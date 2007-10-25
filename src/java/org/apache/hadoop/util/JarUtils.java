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
package org.apache.hadoop.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JarUtils {

  private JarUtils() {}                           // no public ctor

  private static final Log LOG = LogFactory.getLog(JarUtils.class);

  /**
   * <p>Recursive method that get a listing of all files inside of a directory.
   * This is used by the {@link #jar(File, File)} method to get all files that
   * need to be included in the jar.</p>
   * 
   * @param dir The input directory in which to find files.
   * @param toJarList A listing of all files found.
   * 
   * @return File[] An array of files to be jared.
   */
  private static File[] getToBeJared(File dir, List <File> toJarList) {

    // look through the children of the directory
    File[] children = dir.listFiles();
    for (int i = 0; i < children.length; i++) {

      // add files that exist to the jar list, if directory then recurse
      File child = children[i];
      if (child != null && child.exists()) {
        if (!child.isDirectory()) {
          toJarList.add(child);
        } else {
          getToBeJared(child, toJarList);
        }
      }
    }

    return toJarList.toArray(new File[toJarList.size()]);
  }

  /**
   * <p>Rudimentary check for whether a file is a zip or jar file.  It seems that
   * most jar and zip file all have the same magic number 504b03040a.  This 
   * method simply reads the first 5 bytes of the file passed and checks it 
   * agains the magic number.</p>
   * 
   * @param file The file to check.
   * @return True If the first 5 bytes of the file matches 504b03040a.
   * 
   * @throws IOException If there is an error reading the file.
   */
  public static boolean isJarOrZipFile(File file)
    throws IOException {

    // read the first 5 bytes of the file and close the stream
    FileInputStream fis = new FileInputStream(file);
    byte[] buffer = new byte[2];
    try {      
      if (fis.read(buffer) != 2)
        return false;
    }
    finally {
      fis.close();
    }

    // check it against the magic number 504b
    boolean isJarOrZip = (buffer[0] == (byte)0x50 && buffer[1] == (byte)0x4b);

    return isJarOrZip;
  }

  /**
   * <p>Returns the path the resource will use within the jar file.  This is
   * the path from the parent directory being jared to the resource itself.
   * We use this path to ensure that resources are unjared into the correct
   * directory structure.</p>
   * 
   * @param dirToJar The directory being jared.
   * @param currentResource The current resource.
   * 
   * @return String The path to use in the jar file for the resource.
   */
  public static String getJarPath(File dirToJar, File currentResource) {

    // find the path from the resource up recursively to the directory
    // being jared
    String currentPath = null;
    while (!currentResource.toString().equals(dirToJar.toString())) {
      currentPath = currentResource.getName()
        + (currentPath == null ? "" : File.separator + currentPath);
      currentResource = currentResource.getParentFile();
    }

    return currentPath;
  }

  /**
   * Copies the contents of one jar to a jar output stream.
   * 
   * @param jarFile The input jar.
   * @param jarOut The output jar.
   * 
   * @throws IOException If an error occurs while copying jar contents.
   */
  public static void copyJarContents(File jarFile, JarOutputStream jarOut)
    throws IOException {

    // get the input stream to read the input jar
    FileInputStream in = new FileInputStream(jarFile);
    byte buffer[] = new byte[1024];
    JarInputStream jarIn = new JarInputStream(in);
    JarEntry current = null;

    // loop through the jar entries in the input jar
    while ((current = jarIn.getNextJarEntry()) != null) {

      // put each entry in the output jar stream
      jarOut.putNextEntry(current);
      long numBytes = current.getSize();
      int totalRead = 0;

      // copy the contents over to the stream
      while (totalRead <= numBytes) {
        int numRead = jarIn.read(buffer, 0, buffer.length);
        if (numRead <= 0) {
          break;
        }
        else {
          totalRead += numRead;
        }
        jarOut.write(buffer, 0, numRead);
      }
    }

    // close the input jar stream
    jarIn.close();
    in.close();
  }

  /**
   * <p>Creates an output jar with all of the file resources passed.  Resources
   * can be both files and directories.  If the resources is a directory then 
   * the entire directory and all of its contents are copied into the new jar.
   * </p>
   * 
   * <p>All resources are copied from their original locations so there is no
   * need for any temporary directory.  This also has much better performance 
   * than copying resources to a temporary directory and then jaring.</p>
   * 
   * <p>The uncompress option specifies whether jar and zip resources should
   * be uncompressed into the new jar file or should be included as whole file
   * units in the new jar file.
   */
  public static void jarAll(File[] resources, File outputJar, boolean uncompress)
    throws IOException {

    // if we have resources to jar up
    if (resources != null && resources.length > 0) {

      // create a jar file and a manifest to keep track of what we are adding
      FileOutputStream stream = new FileOutputStream(outputJar);
      JarOutputStream jarOut = new JarOutputStream(stream, new Manifest());
      Set <String> jared = new HashSet <String>();

      for (int i = 0; i < resources.length; i++) {

        // if we have an existing resource
        File curRes = resources[i];
        if (curRes != null && curRes.exists()) {

          if (curRes.isFile()) {

            // see if the file is a compressed file or not, assumes .jar and 
            // .zip are compressed to save time
            String resName = curRes.getName();
            boolean isJarOrZip = resName.endsWith(".jar")
              || resName.endsWith(".zip") || isJarOrZipFile(curRes);

            // if we are uncompressing jars and the resource is a jar or zip
            if (uncompress && isJarOrZip) {

              // copy the contents of the jar directly to the output jar
              // this avoids having to make a temp directory, we don't use
              // the utility method because we want to filter what we copy
              // and not overwrite resources
              FileInputStream in = new FileInputStream(curRes);
              byte buffer[] = new byte[1024];
              JarFile jarFile = new JarFile(curRes);
              Enumeration <JarEntry> entries = jarFile.entries();

              // loop through the entries in the jar to copy
              while (entries.hasMoreElements()) {

                // see if the entry already exists in our output jar
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                boolean exists = jared.contains(entryName);

                // we don't copy over anything that already exists (i.e. no 
                // overwriting), we also don't copy over empty directories as 
                // any directory will be created with the resources they contain
                // and finally we don't copy over manifest files
                if (!exists && !entry.isDirectory()
                  && !entryName.equals(jarFile.MANIFEST_NAME)) {

                  InputStream entryIn = jarFile.getInputStream(entry);
                  jarOut.putNextEntry(entry);
                  int totalRead = 0;

                  int numRead = 0;
                  while ((numRead = entryIn.read(buffer, 0, buffer.length)) != -1) {
                    totalRead += numRead;
                    jarOut.write(buffer, 0, numRead);
                  }

                  // log the entry added and close the entry input stream
                  LOG.info("Adding " + entry + ":" + totalRead + " to "
                    + outputJar.getName() + ".");
                  entryIn.close();

                  // add the entry paths
                  jared.add(entryName);
                }
              }

              // close the input jar
              jarFile.close();
              in.close();
            }
            else {

              // we are not uncompressing jar resources or the resource is not
              // a compressed file, we just copy over to output jar.  
              String resourceName = curRes.getName();
              boolean exists = jared.contains(resourceName);

              // if the resource already exists, then we don't overwrite
              if (!exists) {

                // add the jar entry
                JarEntry jarAdd = new JarEntry(resourceName);
                jarAdd.setTime(curRes.lastModified());
                jarOut.putNextEntry(jarAdd);

                LOG.info("Adding " + resourceName + ":" + curRes.length()
                  + " to " + outputJar.getName() + ".");

                // create a stream to copy the resource
                FileInputStream in = new FileInputStream(curRes);
                byte buffer[] = new byte[1024];
                long numBytes = curRes.length();
                int totalRead = 0;

                // copy the resource to the output stream
                while (totalRead <= numBytes) {
                  int numRead = in.read(buffer, 0, buffer.length);
                  if (numRead <= 0) {
                    break;
                  }
                  else {
                    totalRead += numRead;
                  }
                  jarOut.write(buffer, 0, numRead);
                }

                // flush the current jar output stream
                jarOut.flush();

                // add the entry paths
                jared.add(resourceName);

                // close the copier stream
                in.close();
              }
            }
          }
          else if (curRes.isDirectory()) {

            // get a listing of the files to be jared
            File[] toBeJared = getToBeJared(curRes, new ArrayList <File>());
            byte buffer[] = new byte[1024];

            // loop through the files
            for (int k = 0; k < toBeJared.length; k++) {

              // for each entry create the correct jar path name
              File current = toBeJared[k];
              String currentPath = getJarPath(curRes, current);
              boolean exists = jared.contains(currentPath);

              // don't overwrite resources
              if (!exists) {

                // Add entry to the jar
                JarEntry jarAdd = new JarEntry(currentPath);
                jarAdd.setTime(toBeJared[k].lastModified());
                jarOut.putNextEntry(jarAdd);

                LOG.info("Adding " + currentPath + ":" + curRes.length()
                  + " to " + outputJar.getName() + ".");

                // create a stream to copy the file
                FileInputStream in = new FileInputStream(toBeJared[k]);
                while (true) {
                  int numRead = in.read(buffer, 0, buffer.length);
                  if (numRead <= 0) {
                    break;
                  }
                  jarOut.write(buffer, 0, numRead);
                }

                // add the entry paths
                jared.add(currentPath);

                // close the copier stream
                in.close();
              }
            }
          } // end file or directory
        }
      } // end resources loop

      // close the output jar streams
      jarOut.close();
      stream.close();
    }
  }

  /** 
   * <p>Creates a Jar file from the directory passed.  The jar file will contain
   * all file inside the directory, but not the directory itself.</p>
   * 
   * @param dir The directory to jar.
   * @param jarFile The output jar archive file.
   * 
   * @throws IOException  If a problem occurs while jarring the directory.
   */
  public static void jar(File dir, File jarFile)
    throws IOException {

    // if the directory to jar doesn't exist or isn't a directory
    if (dir == null || !dir.isDirectory()) {
      throw new IllegalArgumentException("Input must be an existing directory.");
    }

    // get a listing of the files to be jared
    File[] toBeJared = getToBeJared(dir, new ArrayList <File>());

    byte buffer[] = new byte[1024];
    FileOutputStream stream = new FileOutputStream(jarFile);
    JarOutputStream out = new JarOutputStream(stream, new Manifest());

    // loop through the files
    for (int i = 0; i < toBeJared.length; i++) {

      // for each entry create the correct jar path name
      File current = toBeJared[i];
      String currentPath = null;
      while (!current.toString().equals(dir.toString())) {
        currentPath = current.getName()
          + (currentPath == null ? "" : File.separator + currentPath);
        current = current.getParentFile();
      }

      // Add entry to the jar
      JarEntry jarAdd = new JarEntry(currentPath);
      jarAdd.setTime(toBeJared[i].lastModified());
      out.putNextEntry(jarAdd);

      // Write file to archive
      FileInputStream in = new FileInputStream(toBeJared[i]);
      while (true) {
        int nRead = in.read(buffer, 0, buffer.length);
        if (nRead <= 0)
          break;
        out.write(buffer, 0, nRead);
      }

      in.close();
    }

    out.close();
    stream.close();
  }

  /** Unpack a jar file into a directory. */
  public static void unJar(File jarFile, File toDir)
    throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry)entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create "
                  + file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            }
            finally {
              out.close();
            }
          }
          finally {
            in.close();
          }
        }
      }
    }
    finally {
      jar.close();
    }
  }

  /**
   * <p>Returns the full path to a jar on the classpath that matches name given. 
   * This method will start with the classloader containing the current class 
   * and will progressively search all parent classloaders until a match is
   * found or all resources are exhausted.  Only the first match is returned.
   * </p>
   * 
   * @param currentClass A class from the classloader to start searching.
   * @param jarName The jar to search for.
   * 
   * @return String The full path to the first matching jar or null if no 
   * matching jar is found.
   */
  public static String findJar(Class currentClass, String jarName) {

    ClassLoader loader = currentClass.getClassLoader();

    try {
      while (loader != null) {
        if (loader instanceof URLClassLoader) {
          URLClassLoader ucl = (URLClassLoader)loader;
          URL[] urls = ucl.getURLs();
          for (int i = 0; i < urls.length; i++) {
            URL url = urls[i];
            String fullpath = url.toString();
            if (!fullpath.endsWith("/")) {
              String fileName = fullpath.substring(fullpath.lastIndexOf("/") + 1);
              if (jarName.equals(fileName)) {
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                  toReturn = toReturn.substring("file:".length());
                }
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
              }
            }
          }
        }
        loader = loader.getParent();
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  /** 
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * 
   * @param my_class the class to find
   * @return a jar file that contains the class, or null
   * @throws IOException
   */
  public static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
        URL url = (URL)itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
