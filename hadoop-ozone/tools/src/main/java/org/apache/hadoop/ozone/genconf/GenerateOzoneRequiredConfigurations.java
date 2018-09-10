/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.genconf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * GenerateOzoneRequiredConfigurations - A tool to generate ozone-site.xml<br>
 * This tool generates an ozone-site.xml with minimally required configs.
 * This tool can be invoked as follows:<br>
 * <ul>
 * <li>ozone genconf -output <Path to output file></li>
 * <li>ozone genconf -help</li>
 * </ul>
 */
public final class GenerateOzoneRequiredConfigurations {

  private static final String OUTPUT = "-output";
  private static final String HELP = "-help";
  private static final String USAGE = "Usage: \nozone genconf "
      + OUTPUT + " <Path to output file> \n"
      + "ozone genconf "
      + HELP;
  private static final int SUCCESS = 0;
  private static final int FAILURE = 1;

  private GenerateOzoneRequiredConfigurations() {

  }
  /**
   * Entry point for using genconf tool.
   *
   * @param args
   * @throws JAXBException
   */
  public static void main(String[] args) {

    try {
      if (args.length == 0) {
        System.out.println(USAGE);
        System.exit(1);
      }

      switch (args[0]) {
      case OUTPUT:
        if (args.length > 1) {
          int result = generateConfigurations(args[1]);
        } else {
          System.out.println("Path to output file is mandatory");
          System.out.println(USAGE);
          System.exit(1);
        }
        break;

      case HELP:
        System.out.println(USAGE);
        System.exit(0);
        break;

      default:
        System.out.println(USAGE);
        System.exit(1);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Check if the path is valid directory.
   *
   * @param path
   * @return true, if path is valid directory, else return false
   */
  public static boolean isValidPath(String path) {
    try {
      return Files.isDirectory(Paths.get(path));
    } catch (InvalidPathException | NullPointerException ex) {
      return false;
    }
  }

  /**
   * Check if user has permission to write in the specified path.
   *
   * @param path
   * @return true, if the user has permission to write, else returns false
   */
  public static boolean canWrite(String path) {
    File file = new File(path);
    return file.canWrite();
  }

  /**
   * Generate ozone-site.xml at specified path.
   *
   * @param path
   * @return SUCCESS(0) if file can be generated, else returns FAILURE(1)
   * @throws JAXBException
   */
  public static int generateConfigurations(String path) throws JAXBException {

    if (!isValidPath(path)) {
      System.out.println("Invalid directory path.");
      return FAILURE;
    }

    if (!canWrite(path)) {
      System.out.println("Insufficient permission.");
      return FAILURE;
    }

    OzoneConfiguration oc = new OzoneConfiguration();

    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = OzoneConfiguration.class.getClassLoader();
    }
    URL url = cL.getResource("ozone-default.xml");

    List<OzoneConfiguration.Property> allProperties =
        oc.readPropertyFromXml(url);

    List<OzoneConfiguration.Property> requiredProperties = new ArrayList<>();

    for (OzoneConfiguration.Property p : allProperties) {
      if (p.getTag() != null && p.getTag().contains("REQUIRED")) {
        requiredProperties.add(p);
      }
    }

    OzoneConfiguration.XMLConfiguration requiredConfig =
        new OzoneConfiguration.XMLConfiguration();
    requiredConfig.setProperties(requiredProperties);

    JAXBContext context =
        JAXBContext.newInstance(OzoneConfiguration.XMLConfiguration.class);
    Marshaller m = context.createMarshaller();
    m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
    m.marshal(requiredConfig, new File(path, "ozone-site.xml"));

    System.out.println("ozone-site.xml has been generated at " + path);

    return SUCCESS;
  }
}
