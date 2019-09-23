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

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.PicocliException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.io.IOException;
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
 * <li>ozone genconf {@literal <Path to output file>}</li>
 * <li>ozone genconf --help</li>
 * <li>ozone genconf -h</li>
 * </ul>
 */
@Command(
    name = "ozone genconf",
    description = "Tool to generate template ozone-site.xml",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public final class GenerateOzoneRequiredConfigurations extends GenericCli {

  @Parameters(arity = "1..1",
      description = "Directory path where ozone-site file should be generated.")
  private String path;

  /**
   * Entry point for using genconf tool.
   *
   * @param args
   *
   */
  public static void main(String[] args) throws Exception {
    new GenerateOzoneRequiredConfigurations().run(args);
  }

  @Override
  public Void call() throws Exception {
    generateConfigurations(path);
    return null;
  }

  /**
   * Generate ozone-site.xml at specified path.
   * @param path
   * @throws PicocliException
   * @throws JAXBException
   */
  public static void generateConfigurations(String path) throws
      PicocliException, JAXBException, IOException {

    if (!isValidPath(path)) {
      throw new PicocliException("Invalid directory path.");
    }

    if (!canWrite(path)) {
      throw new PicocliException("Insufficient permission.");
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
        if (p.getName().equalsIgnoreCase(OzoneConfigKeys.OZONE_ENABLED)) {
          p.setValue(String.valueOf(Boolean.TRUE));
        } else if (p.getName().equalsIgnoreCase(
            OzoneConfigKeys.OZONE_METADATA_DIRS)) {
          p.setValue(System.getProperty(OzoneConsts.JAVA_TMP_DIR));
        } else if (p.getName().equalsIgnoreCase(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY)
            || p.getName().equalsIgnoreCase(ScmConfigKeys.OZONE_SCM_NAMES)
            || p.getName().equalsIgnoreCase(
              ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)) {
          p.setValue(OzoneConsts.LOCALHOST);
        }

        requiredProperties.add(p);
      }
    }

    OzoneConfiguration.XMLConfiguration requiredConfig =
        new OzoneConfiguration.XMLConfiguration();
    requiredConfig.setProperties(requiredProperties);

    File output = new File(path, "ozone-site.xml");
    if(output.createNewFile()){
      JAXBContext context =
          JAXBContext.newInstance(OzoneConfiguration.XMLConfiguration.class);
      Marshaller m = context.createMarshaller();
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      m.marshal(requiredConfig, output);

      System.out.println("ozone-site.xml has been generated at " + path);
    } else {
      System.out.printf("ozone-site.xml already exists at %s and " +
          "will not be overwritten%n", path);
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
      return Boolean.FALSE;
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

}
