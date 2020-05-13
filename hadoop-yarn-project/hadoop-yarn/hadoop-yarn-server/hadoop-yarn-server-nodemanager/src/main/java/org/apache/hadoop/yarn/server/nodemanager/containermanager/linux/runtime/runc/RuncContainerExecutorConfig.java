/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc;

import org.apache.hadoop.classification.InterfaceStability;
import org.codehaus.jackson.annotate.JsonRawValue;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;
import java.util.Map;

/**
 *  This class is used by the
 *  {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime}
 *  to pass a JSON
 *  object to the container-executor. The first level of the JSON is comprised
 *  of data that is specific to the container-executor. Included in this is
 *  a JSON object named ociRuntimeConfig that mirrors the
 *  OCI runtime specification.
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_DEFAULT)
@InterfaceStability.Unstable
public class RuncContainerExecutorConfig {
  final private String version;
  final private String runAsUser;
  final private String username;
  final private String containerId;
  final private String applicationId;
  final private String pidFile;
  final private String containerScriptPath;
  final private String containerCredentialsPath;
  final private int https;
  final private String keystorePath;
  final private String truststorePath;
  final private List<String> localDirs;
  final private List<String> logDirs;
  final private List<OCILayer> layers;
  final private int reapLayerKeepCount;
  final private OCIRuntimeConfig ociRuntimeConfig;

  public String getVersion() {
    return version;
  }

  public String getRunAsUser() {
    return runAsUser;
  }

  public String getUsername() {
    return username;
  }

  public String getContainerId() {
    return containerId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getPidFile() {
    return pidFile;
  }

  public String getContainerScriptPath() {
    return containerScriptPath;
  }

  public String getContainerCredentialsPath() {
    return containerCredentialsPath;
  }

  public int getHttps() {
    return https;
  }
  public String getKeystorePath() {
    return keystorePath;
  }
  public String getTruststorePath() {
    return truststorePath;
  }
  public List<String> getLocalDirs() {
    return localDirs;
  }

  public List<String> getLogDirs() {
    return logDirs;
  }

  public List<OCILayer> getLayers() {
    return layers;
  }

  public int getReapLayerKeepCount() {
    return reapLayerKeepCount;
  }

  public OCIRuntimeConfig getOciRuntimeConfig() {
    return ociRuntimeConfig;
  }

  public RuncContainerExecutorConfig() {
    this(null, null, null, null, null, null, null, null, 0, null,
        null, null, null, null, 0, null);
  }

  public RuncContainerExecutorConfig(String runAsUser, String username,
      String containerId, String applicationId,
      String pidFile, String containerScriptPath,
      String containerCredentialsPath,
      int https, String keystorePath, String truststorePath,
      List<String> localDirs,
      List<String> logDirs, List<OCILayer> layers, int reapLayerKeepCount,
      OCIRuntimeConfig ociRuntimeConfig) {
    this("0.1", runAsUser, username, containerId, applicationId, pidFile,
        containerScriptPath, containerCredentialsPath, https, keystorePath,
        truststorePath, localDirs, logDirs,
        layers, reapLayerKeepCount, ociRuntimeConfig);
  }

  public RuncContainerExecutorConfig(String version, String runAsUser,
      String username, String containerId, String applicationId,
      String pidFile, String containerScriptPath,
      String containerCredentialsPath,
      int https, String keystorePath, String truststorePath,
      List<String> localDirs,
      List<String> logDirs, List<OCILayer> layers, int reapLayerKeepCount,
      OCIRuntimeConfig ociRuntimeConfig) {
    this.version = version;
    this.runAsUser = runAsUser;
    this.username = username;
    this.containerId = containerId;
    this.applicationId = applicationId;
    this.pidFile = pidFile;
    this.containerScriptPath = containerScriptPath;
    this.containerCredentialsPath = containerCredentialsPath;
    this.https = https;
    this.keystorePath = keystorePath;
    this.truststorePath = truststorePath;
    this.localDirs = localDirs;
    this.logDirs = logDirs;
    this.layers = layers;
    this.reapLayerKeepCount = reapLayerKeepCount;
    this.ociRuntimeConfig = ociRuntimeConfig;
  }

  /**
   * This class is a Java representation of an OCI image layer.
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  @InterfaceStability.Unstable
  public static class OCILayer {
    final private String mediaType;
    final private String path;

    public String getMediaType() {
      return mediaType;
    }

    public String getPath() {
      return path;
    }

    public OCILayer(String mediaType, String path) {
      this.mediaType = mediaType;
      this.path = path;
    }

    public OCILayer() {
      this(null, null);
    }
  }

  /**
   * This class is a Java representation of the OCI Runtime Specification.
   */
  @InterfaceStability.Unstable
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public static class OCIRuntimeConfig {
    final private OCIRootConfig root;
    final private List<OCIMount> mounts;
    final private OCIProcessConfig process;
    final private OCIHooksConfig hooks;
    final private OCIAnnotationsConfig annotations;
    final private OCILinuxConfig linux;

    public OCIRootConfig getRoot() {
      return root;
    }

    public List<OCIMount> getMounts() {
      return mounts;
    }

    public OCIProcessConfig getProcess() {
      return process;
    }

    public String getHostname() {
      return hostname;
    }

    public OCIHooksConfig getHooks() {
      return hooks;
    }

    public OCIAnnotationsConfig getAnnotations() {
      return annotations;
    }

    public OCILinuxConfig getLinux() {
      return linux;
    }

    final private String hostname;

    public OCIRuntimeConfig() {
      this(null, null, null, null, null, null, null);
    }


    public OCIRuntimeConfig(OCIRootConfig root, List<OCIMount> mounts,
        OCIProcessConfig process, String hostname,
        OCIHooksConfig hooks,
        OCIAnnotationsConfig annotations,
        OCILinuxConfig linux) {
      this.root = root;
      this.mounts = mounts;
      this.process = process;
      this.hostname = hostname;
      this.hooks = hooks;
      this.annotations = annotations;
      this.linux= linux;
    }

    /**
     * This class is a Java representation of the oci root config section
     * of the OCI Runtime Specification.
     */
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public static class OCIRootConfig {
      public String getPath() {
        return path;
      }

      public boolean isReadonly() {
        return readonly;
      }

      final private String path;
      final private boolean readonly;

      public OCIRootConfig(String path, boolean readonly) {
        this.path = path;
        this.readonly = readonly;
      }

      public OCIRootConfig() {
        this(null, false);
      }
    }

    /**
     * This class is a Java representation of the oci mount section
     * of the OCI Runtime Specification.
     */
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public static class OCIMount {
      final private String destination;
      final private String type;
      final private String source;
      final private List<String> options;

      public String getDestination() {
        return destination;
      }

      public String getType() {
        return type;
      }

      public String getSource() {
        return source;
      }

      public List<String> getOptions() {
        return options;
      }

      public OCIMount(String destination, String type, String source,
          List<String> options) {
        this.destination = destination;
        this.type = type;
        this.source = source;
        this.options = options;
      }

      public OCIMount(String destination, String source, List<String> options) {
        this.destination = destination;
        this.type = null;
        this.source = source;
        this.options = options;
      }

      public OCIMount() {
        this(null, null, null, null);
      }
    }


    /**
     * This class is a Java representation of the oci process section
     * of the OCI Runtime Specification.
     */
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public static class OCIProcessConfig {
      final private boolean terminal;
      final private ConsoleSize consoleSize;
      final private String cwd;
      final private List<String> env;
      final private List<String> args;
      final private RLimits rlimits;
      final private String apparmorProfile;
      final private Capabilities capabilities;
      final private boolean noNewPrivileges;
      final private int oomScoreAdj;
      final private String selinuxLabel;
      final private User user;

      public boolean isTerminal() {
        return terminal;
      }

      public ConsoleSize getConsoleSize() {
        return consoleSize;
      }

      public String getCwd() {
        return cwd;
      }

      public List<String> getEnv() {
        return env;
      }

      public List<String> getArgs() {
        return args;
      }

      public RLimits getRlimits() {
        return rlimits;
      }

      public String getApparmorProfile() {
        return apparmorProfile;
      }

      public Capabilities getCapabilities() {
        return capabilities;
      }

      public boolean isNoNewPrivileges() {
        return noNewPrivileges;
      }

      public int getOomScoreAdj() {
        return oomScoreAdj;
      }

      public String getSelinuxLabel() {
        return selinuxLabel;
      }

      public User getUser() {
        return user;
      }


      public OCIProcessConfig(boolean terminal, ConsoleSize consoleSize,
          String cwd, List<String> env, List<String> args, RLimits rlimits,
          String apparmorProfile, Capabilities capabilities,
          boolean noNewPrivileges, int oomScoreAdj, String selinuxLabel,
          User user) {
        this.terminal = terminal;
        this.consoleSize = consoleSize;
        this.cwd = cwd;
        this.env = env;
        this.args = args;
        this.rlimits = rlimits;
        this.apparmorProfile = apparmorProfile;
        this.capabilities = capabilities;
        this.noNewPrivileges = noNewPrivileges;
        this.oomScoreAdj = oomScoreAdj;
        this.selinuxLabel = selinuxLabel;
        this.user = user;
      }

      public OCIProcessConfig() {
        this(false, null, null, null, null, null, null, null,
            false, 0, null, null);
      }


      /**
       * This class is a Java representation of the console size section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class ConsoleSize {
        public int getHeight() {
          return height;
        }

        public int getWidth() {
          return width;
        }

        final private int height;

        public ConsoleSize(int height, int width) {
          this.height = height;
          this.width = width;
        }

        public ConsoleSize() {
          this(0, 0);
        }

        final private int width;
      }

      /**
       * This class is a Java representation of the rlimits section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class RLimits {
        public String getType() {
          return type;
        }

        public long getSoft() {
          return soft;
        }

        public long getHard() {
          return hard;
        }

        final private String type;

        public RLimits(String type, long soft, long hard) {
          this.type = type;
          this.soft = soft;
          this.hard = hard;
        }

        public RLimits() {
          this(null, 0, 0);
        }

        final private long soft;
        final private long hard;
      }

      /**
       * This class is a Java representation of the capabilities section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class Capabilities {
        final private List<String> effective;
        final private List<String> bounding;
        final private List<String> inheritable;
        final private List<String> permitted;
        final private List<String> ambient;

        public List<String> getEffective() {
          return effective;
        }

        public List<String> getBounding() {
          return bounding;
        }

        public List<String> getInheritable() {
          return inheritable;
        }

        public List<String> getPermitted() {
          return permitted;
        }

        public List<String> getAmbient() {
          return ambient;
        }


        public Capabilities(List<String> effective, List<String> bounding,
            List<String> inheritable, List<String> permitted,
            List<String> ambient) {
          this.effective = effective;
          this.bounding = bounding;
          this.inheritable = inheritable;
          this.permitted = permitted;
          this.ambient = ambient;
        }

        public Capabilities() {
          this(null, null, null, null, null);
        }

      }

      /**
       * This class is a Java representation of the user section
       * of the OCI Runtime Specification.
       */
      public static class User {
        final private int uid;
        final private int gid;
        final private List<Integer> additionalGids;

        public User(int uid, int gid, List<Integer> additionalGids) {
          this.uid = uid;
          this.gid = gid;
          this.additionalGids = additionalGids;
        }

        public User() {
          this(0, 0, null);
        }
      }
    }

    /**
     * This class is a Java representation of the oci hooks section
     * of the OCI Runtime Specification.
     */
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public static class OCIHooksConfig {
      final private List<HookType> prestart;
      final private List<HookType> poststart;
      final private List<HookType> poststop;

      public List<HookType> getPrestart() {
        return prestart;
      }

      public List<HookType> getPoststart() {
        return poststart;
      }

      public List<HookType> getPoststop() {
        return poststop;
      }

      public OCIHooksConfig(List<HookType> prestart, List<HookType> poststart,
          List<HookType> poststop) {
        this.prestart = prestart;
        this.poststart = poststart;
        this.poststop = poststop;
      }

      public OCIHooksConfig() {
        this(null, null, null);
      }

      /**
       * This class is a Java representation of the hook type section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class HookType {
        final private String path;
        final private List<String> args;
        final private List<String> env;
        final private int timeout;

        public String getPath() {
          return path;
        }

        public List<String> getArgs() {
          return args;
        }

        public List<String> getEnv() {
          return env;
        }

        public int getTimeout() {
          return timeout;
        }

        public HookType(String path, List<String> args, List<String> env,
            int timeout) {
          this.path = path;
          this.args = args;
          this.env = env;
          this.timeout = timeout;
        }

        public HookType() {
          this(null, null, null, 0);
        }

      }
    }

    /**
     * This class is a Java representation of the oci annotations config section
     * of the OCI Runtime Specification.
     */
    public static class OCIAnnotationsConfig {
      final private Map<String, String> annotations;

      public OCIAnnotationsConfig(Map<String, String> annotations) {
        this.annotations = annotations;
      }

      public Map<String, String> getAnnotations() {
        return annotations;
      }

      public OCIAnnotationsConfig() {
        this(null);
      }

    }

    /**
     * This class is a Java representation of the oci linux config section
     * of the OCI Runtime Specification.
     */
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public static class OCILinuxConfig {
      final private List<Namespace> namespaces;
      final private List<IDMapping> uidMappings;
      final private List<IDMapping> gidMappings;
      final private List<Device> devices;
      final private String cgroupsPath;
      final private Resources resources;
      final private IntelRdt intelRdt;
      final private Sysctl sysctl;
      @JsonRawValue
      final private String seccomp;
      final private String rootfsPropagation;
      final private List<String> maskedPaths;
      final private List<String> readonlyPaths;
      final private String mountLabel;

      public List<Namespace> getNamespaces() {
        return namespaces;
      }

      public List<IDMapping> getUidMappings() {
        return uidMappings;
      }

      public List<IDMapping> getGidMappings() {
        return gidMappings;
      }

      public List<Device> getDevices() {
        return devices;
      }

      public String getCgroupsPath() {
        return cgroupsPath;
      }

      public Resources getResources() {
        return resources;
      }

      public IntelRdt getIntelRdt() {
        return intelRdt;
      }

      public Sysctl getSysctl() {
        return sysctl;
      }

      public String getSeccomp() {
        return seccomp;
      }

      public String getRootfsPropagation() {
        return rootfsPropagation;
      }

      public List<String> getMaskedPaths() {
        return maskedPaths;
      }

      public List<String> getReadonlyPaths() {
        return readonlyPaths;
      }

      public String getMountLabel() {
        return mountLabel;
      }

      public OCILinuxConfig(List<Namespace> namespaces,
          List<IDMapping> uidMappings,
          List<IDMapping> gidMappings, List<Device> devices,
          String cgroupsPath, Resources resources, IntelRdt intelRdt,
          Sysctl sysctl, String seccomp, String rootfsPropagation,
          List<String> maskedPaths, List<String> readonlyPaths,
          String mountLabel) {
        this.namespaces = namespaces;
        this.uidMappings = uidMappings;
        this.gidMappings = gidMappings;
        this.devices = devices;
        this.cgroupsPath = cgroupsPath;
        this.resources = resources;
        this.intelRdt = intelRdt;
        this.sysctl = sysctl;
        this.seccomp = seccomp;
        this.rootfsPropagation = rootfsPropagation;
        this.maskedPaths = maskedPaths;
        this.readonlyPaths = readonlyPaths;
        this.mountLabel = mountLabel;
      }

      public OCILinuxConfig() {
        this(null, null, null, null, null, null, null, null,
            null, null, null, null, null);
      }

      /**
       * This class is a Java representation of the namespace section
       * of the OCI Runtime Specification.
       */
      public static class Namespace {
        final private String type;
        final private String path;

        public Namespace(String type, String path) {
          this.type = type;
          this.path = path;
        }

        public Namespace() {
          this(null, null);
        }
      }

      /**
       * This class is a Java representation of the idmapping section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class IDMapping {
        final private int containerID;
        final private int hostID;
        final private int size;

        public int getContainerID() {
          return containerID;
        }

        public int getHostID() {
          return hostID;
        }

        public int getSize() {
          return size;
        }

        public IDMapping(int containerID, int hostID, int size) {
          this.containerID = containerID;
          this.hostID = hostID;
          this.size = size;
        }

        public IDMapping() {
          this(0, 0, 0);
        }

      }

      /**
       * This class is a Java representation of the device section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class Device {
        final private String type;
        final private String path;
        final private long major;
        final private long minor;
        final private int fileMode;
        final private int uid;
        final private int gid;

        public String getType() {
          return type;
        }

        public String getPath() {
          return path;
        }

        public long getMajor() {
          return major;
        }

        public long getMinor() {
          return minor;
        }

        public int getFileMode() {
          return fileMode;
        }

        public int getUid() {
          return uid;
        }

        public int getGid() {
          return gid;
        }

        public Device(String type, String path, long major, long minor,
            int fileMode, int uid, int gid) {
          this.type = type;
          this.path = path;
          this.major = major;
          this.minor = minor;
          this.fileMode = fileMode;
          this.uid = uid;
          this.gid = gid;
        }

        public Device() {
          this(null, null, 0, 0, 0, 0, 0);
        }

      }

      /**
       * This class is a Java representation of the resources section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class Resources {
        final private List<Device> device;
        final private Memory memory;
        final private CPU cpu;
        final private BlockIO blockIO;
        final private List<HugePageLimits> hugePageLimits;
        final private Network network;
        final private PID pid;
        final private RDMA rdma;

        public List<Device> getDevice() {
          return device;
        }

        public Memory getMemory() {
          return memory;
        }

        public CPU getCPU() {
          return cpu;
        }

        public BlockIO getBlockIO() {
          return blockIO;
        }

        public List<HugePageLimits> getHugePageLimits() {
          return hugePageLimits;
        }

        public Network getNetwork() {
          return network;
        }

        public PID getPID() {
          return pid;
        }

        public RDMA getRDMA() {
          return rdma;
        }

        public Resources(List<Device> device,
            Memory memory, CPU cpu,
            BlockIO blockIO, List<HugePageLimits> hugePageLimits,
            Network network, PID pid,
            RDMA rdma) {
          this.device = device;
          this.memory = memory;
          this.cpu = cpu;
          this.blockIO = blockIO;
          this.hugePageLimits = hugePageLimits;
          this.network = network;
          this.pid = pid;
          this.rdma = rdma;
        }

        public Resources() {
          this(null, null, null, null, null, null, null, null);
        }

        /**
         * This class is a Java representation of the device section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class Device {
          final private boolean allow;
          final private String type;
          final private long major;
          final private long minor;
          final private String access;

          public boolean isAllow() {
            return allow;
          }

          public String getType() {
            return type;
          }

          public long getMajor() {
            return major;
          }

          public long getMinor() {
            return minor;
          }

          public String getAccess() {
            return access;
          }

          public Device(boolean allow, String type, long major,
              long minor, String access) {
            this.allow = allow;
            this.type = type;
            this.major = major;
            this.minor = minor;
            this.access = access;
          }

          public Device() {
            this(false, null, 0, 0, null);
          }
        }

        /**
         * This class is a Java representation of the memory section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class Memory {
          final private long limit;
          final private long reservation;
          final private long swap;
          final private long kernel;
          final private long kernelTCP;
          final private long swappiness;
          final private boolean disableOOMKiller;

          public long getLimit() {
            return limit;
          }

          public long getReservation() {
            return reservation;
          }

          public long getSwap() {
            return swap;
          }

          public long getKernel() {
            return kernel;
          }

          public long getKernelTCP() {
            return kernelTCP;
          }

          public long getSwappiness() {
            return swappiness;
          }

          public boolean isDisableOOMKiller() {
            return disableOOMKiller;
          }

          public Memory(long limit, long reservation, long swap,
              long kernel, long kernelTCP, long swappiness,
              boolean disableOOMKiller) {
            this.limit = limit;
            this.reservation = reservation;
            this.swap = swap;
            this.kernel = kernel;
            this.kernelTCP = kernelTCP;
            this.swappiness = swappiness;
            this.disableOOMKiller = disableOOMKiller;
          }

          public Memory() {
            this(0, 0, 0, 0, 0, 0, false);
          }
        }

        /**
         * This class is a Java representation of the cpu section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class CPU {
          final private long quota;
          final private long period;
          final private long realtimeRuntime;
          final private long realtimePeriod;
          final private String cpus;
          final private String mems;

          public long getShares() {
            return shares;
          }

          public long getQuota() {
            return quota;
          }

          public long getPeriod() {
            return period;
          }

          public long getRealtimeRuntime() {
            return realtimeRuntime;
          }

          public long getRealtimePeriod() {
            return realtimePeriod;
          }

          public String getCpus() {
            return cpus;
          }

          public String getMems() {
            return mems;
          }

          final private long shares;

          public CPU(long shares, long quota, long period,
              long realtimeRuntime, long realtimePeriod,
              String cpus, String mems) {
            this.shares = shares;
            this.quota = quota;
            this.period = period;
            this.realtimeRuntime = realtimeRuntime;
            this.realtimePeriod = realtimePeriod;
            this.cpus = cpus;
            this.mems = mems;
          }

          public CPU() {
            this(0, 0, 0, 0, 0, null, null);
          }
        }

        /**
         * This class is a Java representation of the blockio section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class BlockIO {
          final private int weight;
          final private int leafWeight;
          final private List<WeightDevice> weightDevices;
          final private List<ThrottleDevice> throttleReadBpsDevice;
          final private List<ThrottleDevice> throttleWriteBpsDevice;
          final private List<ThrottleDevice> throttleReadIOPSDevice;
          final private List<ThrottleDevice> throttleWriteIOPSDevice;

          public int getWeight() {
            return weight;
          }

          public int getLeafWeight() {
            return leafWeight;
          }

          public List<WeightDevice> getWeightDevices() {
            return weightDevices;
          }

          public List<ThrottleDevice> getThrottleReadBpsDevice() {
            return throttleReadBpsDevice;
          }

          public List<ThrottleDevice> getThrottleWriteBpsDevice() {
            return throttleWriteBpsDevice;
          }

          public List<ThrottleDevice> getThrottleReadIOPSDevice() {
            return throttleReadIOPSDevice;
          }

          public List<ThrottleDevice> getThrottleWriteIOPSDevice() {
            return throttleWriteIOPSDevice;
          }

          public BlockIO(int weight, int leafWeight,
              List<WeightDevice> weightDevices,
              List<ThrottleDevice> throttleReadBpsDevice,
              List<ThrottleDevice> throttleWriteBpsDevice,
              List<ThrottleDevice> throttleReadIOPSDevice,
              List<ThrottleDevice> throttleWriteIOPSDevice) {
            this.weight = weight;
            this.leafWeight = leafWeight;
            this.weightDevices = weightDevices;
            this.throttleReadBpsDevice = throttleReadBpsDevice;
            this.throttleWriteBpsDevice = throttleWriteBpsDevice;
            this.throttleReadIOPSDevice = throttleReadIOPSDevice;
            this.throttleWriteIOPSDevice = throttleWriteIOPSDevice;
          }

          public BlockIO() {
            this(0, 0, null, null, null, null, null);
          }

          /**
           * This class is a Java representation of the weight device section
           * of the OCI Runtime Specification.
           */
          @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
          public static class WeightDevice {
            final private long major;
            final private long minor;
            final private int weight;
            final private int leafWeight;

            public long getMajor() {
              return major;
            }

            public long getMinor() {
              return minor;
            }

            public int getWeight() {
              return weight;
            }

            public int getLeafWeight() {
              return leafWeight;
            }

            public WeightDevice(long major, long minor, int weight,
                int leafWeight) {
              this.major = major;
              this.minor = minor;
              this.weight = weight;
              this.leafWeight = leafWeight;
            }

            public WeightDevice() {
              this(0, 0, 0, 0);
            }
          }

          /**
           * This class is a Java representation of the throttle device section
           * of the OCI Runtime Specification.
           */
          @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
          public static class ThrottleDevice {
            final private long major;
            final private long minor;
            final private long rate;

            public long getMajor() {
              return major;
            }

            public long getMinor() {
              return minor;
            }

            public long getRate() {
              return rate;
            }

            public ThrottleDevice(long major, long minor, long rate) {
              this.major = major;
              this.minor = minor;
              this.rate = rate;
            }

            public ThrottleDevice() {
              this(0, 0, 0);
            }
          }
        }

        /**
         * This class is a Java representation of the huge page limits section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class HugePageLimits {
          final private String pageSize;
          final private long limit;

          public String getPageSize() {
            return pageSize;
          }

          public long getLimit() {
            return limit;
          }

          public HugePageLimits(String pageSize, long limit) {
            this.pageSize = pageSize;
            this.limit = limit;
          }

          public HugePageLimits() {
            this(null, 0);
          }
        }

        /**
         * This class is a Java representation of the network section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class Network {
          final private int classID;
          final private List<NetworkPriority> priorities;

          public int getClassID() {
            return classID;
          }

          public List<NetworkPriority> getPriorities() {
            return priorities;
          }

          public Network(int classID, List<NetworkPriority> priorities) {
            this.classID = classID;
            this.priorities = priorities;
          }

          public Network() {
            this(0, null);
          }

          /**
           * This class is a Java representation of the network priority section
           * of the OCI Runtime Specification.
           */
          @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
          public static class NetworkPriority {
            final private String name;
            final private int priority;

            public String getName() {
              return name;
            }

            public int getPriority() {
              return priority;
            }

            public NetworkPriority(String name, int priority) {
              this.name = name;
              this.priority = priority;
            }

            public NetworkPriority() {
              this(null, 0);
            }
          }
        }

        /**
         * This class is a Java representation of the pid section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class PID {
          final private long limit;

          public long getLimit() {
            return limit;
          }

          public PID(long limit) {
            this.limit = limit;
          }

          public PID() {
            this(0);
          }
        }

        /**
         * This class is a Java representation of the rdma section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class RDMA {
          final private int hcaHandles;
          final private int hcaObjects;

          public int getHcaHandles() {
            return hcaHandles;
          }

          public int getHcaObjects() {
            return hcaObjects;
          }

          public RDMA(int hcaHandles, int hcaObjects) {
            this.hcaHandles = hcaHandles;
            this.hcaObjects = hcaObjects;
          }

          public RDMA() {
            this(0, 0);
          }
        }
      }

      /**
       * This class is a Java representation of the intelrdt section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class IntelRdt {
        final private String closID;
        final private String l3CacheSchema;
        final private String memBwSchema;

        public String getClosID() {
          return closID;
        }

        public String getL3CacheSchema() {
          return l3CacheSchema;
        }

        public String getMemBwSchema() {
          return memBwSchema;
        }

        public IntelRdt(String closID, String l3CacheSchema,
            String memBwSchema) {
          this.closID = closID;
          this.l3CacheSchema = l3CacheSchema;
          this.memBwSchema = memBwSchema;
        }

        public IntelRdt() {
          this(null, null, null);
        }
      }

      /**
       * This class is a Java representation of the sysctl section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class Sysctl {
        // for kernel params
      }

      /**
       * This class is a Java representation of the seccomp section
       * of the OCI Runtime Specification.
       */
      @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
      public static class Seccomp {
        final private String defaultAction;
        final private List<String> architectures;
        final private List<Syscall> syscalls;

        public String getDefaultAction() {
          return defaultAction;
        }

        public List<String> getArchitectures() {
          return architectures;
        }

        public List<Syscall> getSyscalls() {
          return syscalls;
        }

        public Seccomp(String defaultAction, List<String> architectures,
            List<Syscall> syscalls) {
          this.defaultAction = defaultAction;
          this.architectures = architectures;
          this.syscalls = syscalls;
        }

        public Seccomp() {
          this(null, null, null);
        }

        /**
         * This class is a Java representation of the syscall section
         * of the OCI Runtime Specification.
         */
        @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
        public static class Syscall {
          final private List<String> names;
          final private String action;
          final private List<SeccompArg> args;

          public List<String> getNames() {
            return names;
          }

          public String getAction() {
            return action;
          }

          public List<SeccompArg> getArgs() {
            return args;
          }

          public Syscall(List<String> names, String action,
              List<SeccompArg> args) {
            this.names = names;
            this.action = action;
            this.args = args;
          }

          public Syscall() {
            this(null, null, null);
          }

          /**
           * This class is a Java representation of the seccomp arguments
           * of the OCI Runtime Specification.
           */
          @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
          public static class SeccompArg {
            final private int index;
            final private long value;
            final private long valueTwo;
            final private String op;

            public int getIndex() {
              return index;
            }

            public long getValue() {
              return value;
            }

            public long getValueTwo() {
              return valueTwo;
            }

            public String getOp() {
              return op;
            }

            public SeccompArg(int index, long value, long valueTwo, String op) {
              this.index = index;
              this.value = value;
              this.valueTwo = valueTwo;
              this.op = op;
            }

            public SeccompArg() {
              this(0, 0, 0, null);
            }
          }
        }
      }
    }
  }
}
