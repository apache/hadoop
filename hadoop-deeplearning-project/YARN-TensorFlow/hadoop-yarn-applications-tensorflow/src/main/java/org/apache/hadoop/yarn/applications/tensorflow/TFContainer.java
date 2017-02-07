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

package org.apache.hadoop.yarn.applications.tensorflow;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class TFContainer {
    private static final Log LOG = LogFactory.getLog(TFContainer.class);

    private String appName = TFYarnConstants.APP_NAME;
    private ApplicationMaster appMaster;
    public static final String SERVER_PY_PATH = "tf_server.py";
    public static final String SERVER_JAR_PATH = "TFServer.jar";

    public TFContainer(ApplicationMaster am) {
        appMaster = am;
    }

    private void execCmd(String cmd) {
        Process process = null;
        try {
            LOG.info("cmd is " + cmd);
            process = Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            LOG.fatal("cmd running failed", e);
            e.printStackTrace();
        }

        try {
            LOG.info("cmd log--->");
            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {

                LOG.info(line);
                System.out.println(line);
            }
            in.close();
            LOG.info("<---cmd log end");
            process.waitFor();
        } catch (InterruptedException e) {
            LOG.fatal("waiting error ", e);
            e.printStackTrace();
        } catch (IOException e) {
            LOG.info("io exception");
            e.printStackTrace();
        }
    }

    public void addToLocalResources(FileSystem fs, Path dst, String fileDstPath, Map<String, LocalResource> localResources) throws IOException {
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LOG.info("Path " + dst.toString() + "->" + " " + fileDstPath);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        URL.fromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    public void addToLocalResources(FileSystem fs, String srcFilePath, String fileDstPath, Map<String, LocalResource> localResources) throws IOException {

        Path path = new Path(srcFilePath);
        addToLocalResources(fs, path, fileDstPath, localResources);
    }


    public void addToLocalResources(FileSystem fs, String fileSrcPath,
                                    String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                    String resources) throws IOException {

        execCmd("pwd");
        execCmd("ls -l");
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        LOG.info("copy: " + fileSrcPath + " ===> " + dst.toString());
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }

        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        URL.fromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    public Map<String, String> setJavaEnv(Configuration conf, String tfServerJar) {
        // Set the java environment
        Map<String, String> env = new HashMap<String, String>();

        // Add TFServer.jar location to classpath
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");

        // Add hadoop's jar location to classpath
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        if (tfServerJar != null) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(tfServerJar);
        }
        env.put("CLASSPATH", classPathEnv.toString());
        return env;
    }

    public StringBuilder makeCommands(long containerMemory, String clusterSpec, String jobName, int taskIndex) {
        // Set the necessary command to execute on the allocated container
        Vector<CharSequence> vargs = new Vector<CharSequence>(5);
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        //vargs.add("-Xmx" + containerMemory + "m");
        vargs.add("-Xmx" + containerMemory + "m");
        String containerClassName = TFServer.class.getName();
        vargs.add(containerClassName);
        vargs.add("--" + TFServer.OPT_CS + " " + clusterSpec);
        vargs.add("--" + TFServer.OPT_JN + " " + jobName);
        vargs.add("--" + TFServer.OPT_TI + " " + taskIndex);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/TFServer." + ApplicationConstants.STDOUT);
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/TFServer." + ApplicationConstants.STDERR);

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        return command;
    }

}
