/*
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
package org.apache.hadoop.io.compress;

import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that will find the correct codec for a given filename.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompressionCodecFactory {

  public static final Logger LOG =
      LoggerFactory.getLogger(CompressionCodecFactory.class.getName());
  
  private static final ServiceLoader<CompressionCodec> CODEC_PROVIDERS =
    ServiceLoader.load(CompressionCodec.class);

  /**
   * A map from the reversed filename suffixes to the codecs.
   * This is probably overkill, because the maps should be small, but it 
   * automatically supports finding the longest matching suffix. 
   */
  private SortedMap<String, CompressionCodec> codecs = null;

    /**
     * A map from the reversed filename suffixes to the codecs.
     * This is probably overkill, because the maps should be small, but it
     * automatically supports finding the longest matching suffix.
     */
    private Map<String, CompressionCodec> codecsByName = null;

  /**
   * A map from class names to the codecs
   */
  private HashMap<String, CompressionCodec> codecsByClassName = null;

  private void addCodec(CompressionCodec codec) {
    String suffix = codec.getDefaultExtension();
    codecs.put(new StringBuilder(suffix).reverse().toString(), codec);
    codecsByClassName.put(codec.getClass().getCanonicalName(), codec);

    String codecName = codec.getClass().getSimpleName();
    codecsByName.put(StringUtils.toLowerCase(codecName), codec);
    if (codecName.endsWith("Codec")) {
      codecName = codecName.substring(0, codecName.length() - "Codec".length());
      codecsByName.put(StringUtils.toLowerCase(codecName), codec);
    }
  }

  /**
   * Print the extension map out as a string.
   */
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    Iterator<Map.Entry<String, CompressionCodec>> itr = 
      codecs.entrySet().iterator();
    buf.append("{ ");
    if (itr.hasNext()) {
      Map.Entry<String, CompressionCodec> entry = itr.next();
      buf.append(entry.getKey())
          .append(": ")
          .append(entry.getValue().getClass().getName());
      while (itr.hasNext()) {
        entry = itr.next();
        buf.append(", ")
            .append(entry.getKey())
            .append(": ")
            .append(entry.getValue().getClass().getName());
      }
    }
    buf.append(" }");
    return buf.toString();
  }

  /**
   * Get the list of codecs discovered via a Java ServiceLoader, or
   * listed in the configuration. Codecs specified in configuration come
   * later in the returned list, and are considered to override those
   * from the ServiceLoader.
   * @param conf the configuration to look in
   * @return a list of the {@link CompressionCodec} classes
   */
  public static List<Class<? extends CompressionCodec>> getCodecClasses(
      Configuration conf) {
    List<Class<? extends CompressionCodec>> result
      = new ArrayList<Class<? extends CompressionCodec>>();
    // Add codec classes discovered via service loading
    synchronized (CODEC_PROVIDERS) {
      // CODEC_PROVIDERS is a lazy collection. Synchronize so it is
      // thread-safe. See HADOOP-8406.
      for (CompressionCodec codec : CODEC_PROVIDERS) {
        result.add(codec.getClass());
      }
    }
    // Add codec classes from configuration
    String codecsString = conf.get(
        CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY);
    if (codecsString != null) {
      StringTokenizer codecSplit = new StringTokenizer(codecsString, ",");
      while (codecSplit.hasMoreElements()) {
        String codecSubstring = codecSplit.nextToken().trim();
        if (codecSubstring.length() != 0) {
          try {
            Class<?> cls = conf.getClassByName(codecSubstring);
            if (!CompressionCodec.class.isAssignableFrom(cls)) {
              throw new IllegalArgumentException("Class " + codecSubstring +
                                                 " is not a CompressionCodec");
            }
            result.add(cls.asSubclass(CompressionCodec.class));
          } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException("Compression codec " + 
                                               codecSubstring + " not found.",
                                               ex);
          }
        }
      }
    }
    return result;
  }
  
  /**
   * Sets a list of codec classes in the configuration. In addition to any
   * classes specified using this method, {@link CompressionCodec} classes on
   * the classpath are discovered using a Java ServiceLoader.
   * @param conf the configuration to modify
   * @param classes the list of classes to set
   */
  public static void setCodecClasses(Configuration conf,
                                     List<Class> classes) {
    StringBuilder buf = new StringBuilder();
    Iterator<Class> itr = classes.iterator();
    if (itr.hasNext()) {
      Class cls = itr.next();
      buf.append(cls.getName());
      while(itr.hasNext()) {
        buf.append(',')
            .append(itr.next().getName());
      }
    }
    conf.set(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, buf.toString());
  }
  
  /**
   * Find the codecs specified in the config value io.compression.codecs 
   * and register them. Defaults to gzip and deflate.
   */
  public CompressionCodecFactory(Configuration conf) {
    codecs = new TreeMap<String, CompressionCodec>();
    codecsByClassName = new HashMap<String, CompressionCodec>();
    codecsByName = new HashMap<String, CompressionCodec>();
    List<Class<? extends CompressionCodec>> codecClasses =
        getCodecClasses(conf);
    if (codecClasses == null || codecClasses.isEmpty()) {
      addCodec(new GzipCodec());
      addCodec(new DefaultCodec());      
    } else {
      for (Class<? extends CompressionCodec> codecClass : codecClasses) {
        addCodec(ReflectionUtils.newInstance(codecClass, conf));
      }
    }
  }
  
  /**
   * Find the relevant compression codec for the given file based on its
   * filename suffix.
   * @param file the filename to check
   * @return the codec object
   */
  public CompressionCodec getCodec(Path file) {
    CompressionCodec result = null;
    if (codecs != null) {
      String filename = file.getName();
      String reversedFilename =
          new StringBuilder(filename).reverse().toString();
      SortedMap<String, CompressionCodec> subMap = 
        codecs.headMap(reversedFilename);
      if (!subMap.isEmpty()) {
        String potentialSuffix = subMap.lastKey();
        if (reversedFilename.startsWith(potentialSuffix)) {
          result = codecs.get(potentialSuffix);
        }
      }
    }
    return result;
  }
  
  /**
   * Find the relevant compression codec for the codec's canonical class name.
   * @param classname the canonical class name of the codec
   * @return the codec object
   */
  public CompressionCodec getCodecByClassName(String classname) {
    if (codecsByClassName == null) {
      return null;
    }
    return codecsByClassName.get(classname);
  }

    /**
     * Find the relevant compression codec for the codec's canonical class name
     * or by codec alias.
     * <p>
     * Codec aliases are case insensitive.
     * <p>
     * The code alias is the short class name (without the package name).
     * If the short class name ends with 'Codec', then there are two aliases for
     * the codec, the complete short class name and the short class name without
     * the 'Codec' ending. For example for the 'GzipCodec' codec class name the
     * alias are 'gzip' and 'gzipcodec'.
     *
     * @param codecName the canonical class name of the codec
     * @return the codec object
     */
    public CompressionCodec getCodecByName(String codecName) {
      if (codecsByClassName == null) {
        return null;
      }
      CompressionCodec codec = getCodecByClassName(codecName);
      if (codec == null) {
        // trying to get the codec by name in case the name was specified
        // instead a class
        codec = codecsByName.get(StringUtils.toLowerCase(codecName));
      }
      return codec;
    }

    /**
     * Find the relevant compression codec for the codec's canonical class name
     * or by codec alias and returns its implemetation class.
     * <p>
     * Codec aliases are case insensitive.
     * <p>
     * The code alias is the short class name (without the package name).
     * If the short class name ends with 'Codec', then there are two aliases for
     * the codec, the complete short class name and the short class name without
     * the 'Codec' ending. For example for the 'GzipCodec' codec class name the
     * alias are 'gzip' and 'gzipcodec'.
     *
     * @param codecName the canonical class name of the codec
     * @return the codec class
     */
    public Class<? extends CompressionCodec> getCodecClassByName(
        String codecName) {
      CompressionCodec codec = getCodecByName(codecName);
      if (codec == null) {
        return null;
      }
      return codec.getClass();
    }

  /**
   * Removes a suffix from a filename, if it has it.
   * @param filename the filename to strip
   * @param suffix the suffix to remove
   * @return the shortened filename
   */
  public static String removeSuffix(String filename, String suffix) {
    if (filename.endsWith(suffix)) {
      return filename.substring(0, filename.length() - suffix.length());
    }
    return filename;
  }
  
  /**
   * A little test program.
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    boolean encode = false;
    for(int i=0; i < args.length; ++i) {
      if ("-in".equals(args[i])) {
        encode = true;
      } else if ("-out".equals(args[i])) {
        encode = false;
      } else {
        CompressionCodec codec = factory.getCodec(new Path(args[i]));
        if (codec == null) {
          System.out.println("Codec for " + args[i] + " not found.");
        } else { 
          if (encode) {
            CompressionOutputStream out = null;
            java.io.InputStream in = null;
            try {
              out = codec.createOutputStream(
                  new java.io.FileOutputStream(args[i]));
              byte[] buffer = new byte[100];
              String inFilename = removeSuffix(args[i], 
                  codec.getDefaultExtension());
              in = new java.io.FileInputStream(inFilename);
              int len = in.read(buffer);
              while (len > 0) {
                out.write(buffer, 0, len);
                len = in.read(buffer);
              }
            } finally {
              if(out != null) { out.close(); }
              if(in  != null) { in.close(); }
            }
          } else {
            CompressionInputStream in = null;
            try {
              in = codec.createInputStream(
                  new java.io.FileInputStream(args[i]));
              byte[] buffer = new byte[100];
              int len = in.read(buffer);
              while (len > 0) {
                System.out.write(buffer, 0, len);
                len = in.read(buffer);
              }
            } finally {
              if(in != null) { in.close(); }
            }
          }
        }
      }
    }
  }
}
