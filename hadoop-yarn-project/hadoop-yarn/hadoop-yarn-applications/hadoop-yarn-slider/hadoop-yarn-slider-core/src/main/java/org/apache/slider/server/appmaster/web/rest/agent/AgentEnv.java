/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest.agent;

import com.google.gson.annotations.SerializedName;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class AgentEnv {

  /**
   * Various directories, configurable in <code>ambari-agent.ini</code>
   */
  private Directory[] stackFoldersAndFiles = new Directory[0];

  /**
   * Directories that match name <code>/etc/alternatives/*conf</code>
   */
  private Alternative[] alternatives = new Alternative[0];

  /**
   * List of existing users
   */
  private ExistingUser[] existingUsers = new ExistingUser[0];

  /**
   * List of repos
   */
  private String[] existingRepos = new String[0];

  /**
   * List of packages
   */
  private PackageDetail[] installedPackages = new PackageDetail[0];

  /**
   * The host health report
   */
  private HostHealth hostHealth = new HostHealth();

  private Integer umask;

  private Boolean iptablesIsRunning;

  public Integer getUmask() {
    return umask;
  }

  public void setUmask(Integer umask) {
    this.umask = umask;
  }

  public Directory[] getStackFoldersAndFiles() {
    return stackFoldersAndFiles;
  }

  public void setStackFoldersAndFiles(Directory[] dirs) {
    stackFoldersAndFiles = dirs;
  }

  public void setExistingUsers(ExistingUser[] users) {
    existingUsers = users;
  }

  public ExistingUser[] getExistingUsers() {
    return existingUsers;
  }

  public void setAlternatives(Alternative[] dirs) {
    alternatives = dirs;
  }

  public Alternative[] getAlternatives() {
    return alternatives;
  }

  public void setExistingRepos(String[] repos) {
    existingRepos = repos;
  }

  public String[] getExistingRepos() {
    return existingRepos;
  }

  public void setInstalledPackages(PackageDetail[] packages) {
    installedPackages = packages;
  }

  public PackageDetail[] getInstalledPackages() {
    return installedPackages;
  }

  public void setHostHealth(HostHealth healthReport) {
    hostHealth = healthReport;
  }

  public HostHealth getHostHealth() {
    return hostHealth;
  }

  public Boolean getIptablesIsRunning() {
    return iptablesIsRunning;
  }

  public void setIptablesIsRunning(Boolean iptablesIsRunning) {
    this.iptablesIsRunning = iptablesIsRunning;
  }

  public static class HostHealth {
    /**
     * Java processes running on the system.  Default empty array.
     */
    @SerializedName("activeJavaProcs")
    private JavaProc[] activeJavaProcs = new JavaProc[0];

    /**
     * The current time when agent send the host check report
     */
    @SerializedName("agentTimeStampAtReporting")
    private long agentTimeStampAtReporting = 0;

    /**
     * The current time when host check report was received
     */
    @SerializedName("serverTimeStampAtReporting")
    private long serverTimeStampAtReporting = 0;

    /**
     * Live services running on the agent
     */
    @SerializedName("liveServices")
    private LiveService[] liveServices = new LiveService[0];

    public void setAgentTimeStampAtReporting(long currentTime) {
      agentTimeStampAtReporting = currentTime;
    }

    public long getAgentTimeStampAtReporting() {
      return agentTimeStampAtReporting;
    }

    public void setServerTimeStampAtReporting(long currentTime) {
      serverTimeStampAtReporting = currentTime;
    }

    public long getServerTimeStampAtReporting() {
      return serverTimeStampAtReporting;
    }

    public void setActiveJavaProcs(JavaProc[] procs) {
      activeJavaProcs = procs;
    }

    public JavaProc[] getActiveJavaProcs() {
      return activeJavaProcs;
    }

    public void setLiveServices(LiveService[] services) {
      liveServices = services;
    }

    public LiveService[] getLiveServices() {
      return liveServices;
    }
  }

  public static class PackageDetail {
    @SerializedName("name")
    private String pkgName;
    @SerializedName("version")
    private String pkgVersion;
    @SerializedName("repoName")
    private String pkgRepoName;

    public void setName(String name) {
      pkgName = name;
    }

    public String getName() {
      return pkgName;
    }

    public void setVersion(String version) {
      pkgVersion = version;
    }

    public String getVersion() {
      return pkgVersion;
    }

    public void setRepoName(String repoName) {
      pkgRepoName = repoName;
    }

    public String getRepoName() {
      return pkgRepoName;
    }
  }

  /**
   * Represents information about a directory of interest.
   */
  public static class Directory {
    @SerializedName("name")
    private String dirName;
    @SerializedName("type")
    private String dirType;

    public void setName(String name) {
      dirName = name;
    }

    public String getName() {
      return dirName;
    }

    public void setType(String type) {
      dirType = type;
    }

    public String getType() {
      return dirType;
    }
  }

  /**
   * Represents information about running java processes.
   */
  public static class JavaProc {
    @SerializedName("user")
    private String user;
    @SerializedName("pid")
    private int pid = 0;
    @SerializedName("hadoop")
    private boolean is_hadoop = false;
    @SerializedName("command")
    private String command;

    public void setUser(String user) {
      this.user = user;
    }

    public String getUser() {
      return user;
    }

    public void setPid(int pid) {
      this.pid = pid;
    }

    public int getPid() {
      return pid;
    }

    public void setHadoop(boolean hadoop) {
      is_hadoop = hadoop;
    }

    public boolean isHadoop() {
      return is_hadoop;
    }

    public void setCommand(String cmd) {
      command = cmd;
    }

    public String getCommand() {
      return command;
    }
  }

  public static class Alternative {
    @SerializedName("name")
    private String altName;
    @SerializedName("target")
    private String altTarget;

    public void setName(String name) {
      altName = name;
    }

    public String getName() {
      return altName;
    }

    public void setTarget(String target) {
      altTarget = target;
    }

    public String getTarget() {
      return altTarget;
    }
  }

  public static class LiveService {
    @SerializedName("name")
    private String svcName;
    @SerializedName("status")
    private String svcStatus;
    @SerializedName("desc")
    private String svcDesc;

    public void setName(String name) {
      svcName = name;
    }

    public String getName() {
      return svcName;
    }

    public void setStatus(String status) {
      svcStatus = status;
    }

    public String getStatus() {
      return svcStatus;
    }

    public void setDesc(String desc) {
      svcDesc = desc;
    }

    public String getDesc() {
      return svcDesc;
    }
  }

  public static class ExistingUser {
    @SerializedName("name")
    private String name;
    @SerializedName("homeDir")
    private String homeDir;
    @SerializedName("status")
    private String status;

    public void setUserName(String userName) {
      name = userName;
    }

    public String getUserName() {
      return name;
    }

    public void setUserHomeDir(String userHomeDir) {
      homeDir = userHomeDir;
    }

    public String getUserHomeDir() {
      return homeDir;
    }

    public void setUserStatus(String userStatus) {
      status = userStatus;
    }

    public String getUserStatus() {
      return status;
    }
  }
}
