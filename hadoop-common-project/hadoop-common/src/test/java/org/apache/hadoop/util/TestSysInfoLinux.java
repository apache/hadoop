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
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * A JUnit test to test {@link SysInfoLinux}
 * Create the fake /proc/ information and verify the parsing and calculation
 */
public class TestSysInfoLinux {
  /**
   * LinuxResourceCalculatorPlugin with a fake timer
   */
  static class FakeLinuxResourceCalculatorPlugin extends SysInfoLinux {
    static final int SECTORSIZE = 4096;

    long currentTime = 0;
    public FakeLinuxResourceCalculatorPlugin(String procfsMemFile,
                                             String procfsCpuFile,
                                             String procfsStatFile,
			                                       String procfsNetFile,
                                             String procfsDisksFile,
                                             long jiffyLengthInMillis) {
      super(procfsMemFile, procfsCpuFile, procfsStatFile, procfsNetFile,
          procfsDisksFile, jiffyLengthInMillis);
    }
    @Override
    long getCurrentTime() {
      return currentTime;
    }
    public void advanceTime(long adv) {
      currentTime += adv * this.getJiffyLengthInMillis();
    }
    @Override
    int readDiskBlockInformation(String diskName, int defSector) {
      return SECTORSIZE;
    }
  }
  private static final FakeLinuxResourceCalculatorPlugin plugin;
  private static String TEST_ROOT_DIR = GenericTestUtils.getTestDir()
      .getAbsolutePath();
  private static final String FAKE_MEMFILE;
  private static final String FAKE_CPUFILE;
  private static final String FAKE_STATFILE;
  private static final String FAKE_NETFILE;
  private static final String FAKE_DISKSFILE;
  private static final long FAKE_JIFFY_LENGTH = 10L;
  static {
    int randomNum = (new Random()).nextInt(1000000000);
    FAKE_MEMFILE = TEST_ROOT_DIR + File.separator + "MEMINFO_" + randomNum;
    FAKE_CPUFILE = TEST_ROOT_DIR + File.separator + "CPUINFO_" + randomNum;
    FAKE_STATFILE = TEST_ROOT_DIR + File.separator + "STATINFO_" + randomNum;
    FAKE_NETFILE = TEST_ROOT_DIR + File.separator + "NETINFO_" + randomNum;
    FAKE_DISKSFILE = TEST_ROOT_DIR + File.separator + "DISKSINFO_" + randomNum;
    plugin = new FakeLinuxResourceCalculatorPlugin(FAKE_MEMFILE, FAKE_CPUFILE,
                                                   FAKE_STATFILE,
                                                   FAKE_NETFILE,
                                                   FAKE_DISKSFILE,
                                                   FAKE_JIFFY_LENGTH);
  }
  static final String MEMINFO_FORMAT =
    "MemTotal:      %d kB\n" +
    "MemFree:         %d kB\n" +
    "Buffers:        138244 kB\n" +
    "Cached:         947780 kB\n" +
    "SwapCached:     142880 kB\n" +
    "Active:        3229888 kB\n" +
    "Inactive:       %d kB\n" +
    "SwapTotal:     %d kB\n" +
    "SwapFree:      %d kB\n" +
    "Dirty:          122012 kB\n" +
    "Writeback:           0 kB\n" +
    "AnonPages:     2710792 kB\n" +
    "Mapped:          24740 kB\n" +
    "Slab:           132528 kB\n" +
    "SReclaimable:   105096 kB\n" +
    "SUnreclaim:      27432 kB\n" +
    "PageTables:      11448 kB\n" +
    "NFS_Unstable:        0 kB\n" +
    "Bounce:              0 kB\n" +
    "CommitLimit:   4125904 kB\n" +
    "Committed_AS:  4143556 kB\n" +
    "VmallocTotal: 34359738367 kB\n" +
    "VmallocUsed:      1632 kB\n" +
    "VmallocChunk: 34359736375 kB\n" +
    "HugePages_Total:     %d\n" +
    "HugePages_Free:      0\n" +
    "HugePages_Rsvd:      0\n" +
    "Hugepagesize:     2048 kB";

  static final String MEMINFO_FORMAT_2 =
    "MemTotal:       %d kB\n" +
    "MemFree:        %d kB\n" +
    "Buffers:          129976 kB\n" +
    "Cached:         32317676 kB\n" +
    "SwapCached:            0 kB\n" +
    "Active:         88938588 kB\n" +
    "Inactive:       %d kB\n" +
    "Active(anon):   77502200 kB\n" +
    "Inactive(anon):  6385336 kB\n" +
    "Active(file):   11436388 kB\n" +
    "Inactive(file): %d kB\n" +
    "Unevictable:           0 kB\n" +
    "Mlocked:               0 kB\n" +
    "SwapTotal:      %d kB\n" +
    "SwapFree:       %d kB\n" +
    "Dirty:            575864 kB\n" +
    "Writeback:            16 kB\n" +
    "AnonPages:      83886180 kB\n" +
    "Mapped:           108640 kB\n" +
    "Shmem:              1880 kB\n" +
    "Slab:            2413448 kB\n" +
    "SReclaimable:    2194488 kB\n" +
    "SUnreclaim:       218960 kB\n" +
    "KernelStack:       31496 kB\n" +
    "PageTables:       195176 kB\n" +
    "NFS_Unstable:          0 kB\n" +
    "Bounce:                0 kB\n" +
    "WritebackTmp:          0 kB\n" +
    "CommitLimit:    97683468 kB\n" +
    "Committed_AS:   94553560 kB\n" +
    "VmallocTotal:   34359738367 kB\n" +
    "VmallocUsed:      498580 kB\n" +
    "VmallocChunk:   34256922296 kB\n" +
    "HardwareCorrupted: %d kB\n" +
    "AnonHugePages:         0 kB\n" +
    "HugePages_Total:       %d\n" +
    "HugePages_Free:        0\n" +
    "HugePages_Rsvd:        0\n" +
    "HugePages_Surp:        0\n" +
    "Hugepagesize:       2048 kB\n" +
    "DirectMap4k:        4096 kB\n" +
    "DirectMap2M:     2027520 kB\n" +
    "DirectMap1G:    132120576 kB\n";

  static final String CPUINFO_FORMAT =
    "processor : %s\n" +
    "vendor_id : AuthenticAMD\n" +
    "cpu family  : 15\n" +
    "model   : 33\n" +
    "model name  : Dual Core AMD Opteron(tm) Processor 280\n" +
    "stepping  : 2\n" +
    "cpu MHz   : %f\n" +
    "cache size  : 1024 KB\n" +
    "physical id : %s\n" +
    "siblings  : 2\n" +
    "core id   : %s\n" +
    "cpu cores : 2\n" +
    "fpu   : yes\n" +
    "fpu_exception : yes\n" +
    "cpuid level : 1\n" +
    "wp    : yes\n" +
    "flags   : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov " +
    "pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt lm " +
    "3dnowext 3dnow pni lahf_lm cmp_legacy\n" +
    "bogomips  : 4792.41\n" +
    "TLB size  : 1024 4K pages\n" +
    "clflush size  : 64\n" +
    "cache_alignment : 64\n" +
    "address sizes : 40 bits physical, 48 bits virtual\n" +
    "power management: ts fid vid ttp";

  static final String STAT_FILE_FORMAT =
    "cpu  %d %d %d 1646495089 831319 48713 164346 0\n" +
    "cpu0 15096055 30805 3823005 411456015 206027 13 14269 0\n" +
    "cpu1 14760561 89890 6432036 408707910 456857 48074 130857 0\n" +
    "cpu2 12761169 20842 3758639 413976772 98028 411 10288 0\n" +
    "cpu3 12355207 47322 5789691 412354390 70406 213 8931 0\n" +
    "intr 114648668 20010764 2 0 945665 2 0 0 0 0 0 0 0 4 0 0 0 0 0 0\n" +
    "ctxt 242017731764\n" +
    "btime 1257808753\n" +
    "processes 26414943\n" +
    "procs_running 1\n" +
    "procs_blocked 0\n";

  static final String NETINFO_FORMAT =
    "Inter-|   Receive                                                |  Transmit\n"+
    "face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets"+
    "errs drop fifo colls carrier compressed\n"+
    "   lo: 42236310  563003    0    0    0     0          0         0 42236310  563003    " +
    "0    0    0     0       0          0\n"+
    " eth0: %d 3452527    0    0    0     0          0    299787 %d 1866280    0    0    " +
    "0     0       0          0\n"+
    " eth1: %d 3152521    0    0    0     0          0    219781 %d 1866290    0    0    " +
    "0     0       0          0\n";

  static final String DISKSINFO_FORMAT =
      "1       0 ram0 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       1 ram1 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       2 ram2 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       3 ram3 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       4 ram4 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       5 ram5 0 0 0 0 0 0 0 0 0 0 0\n"+
      "1       6 ram6 0 0 0 0 0 0 0 0 0 0 0\n"+
      "7       0 loop0 0 0 0 0 0 0 0 0 0 0 0\n"+
      "7       1 loop1 0 0 0 0 0 0 0 0 0 0 0\n"+
      "8       0 sda 82575678 2486518 %d 59876600 3225402 19761924 %d " +
      "6407705 4 48803346 66227952\n"+
      "8       1 sda1 732 289 21354 787 7 3 32 4 0 769 791"+
      "8       2 sda2 744272 2206315 23605200 6742762 336830 2979630 " +
      "26539520 1424776 4 1820130 8165444\n"+
      "8       3 sda3 81830497 279914 17881852954 53132969 2888558 16782291 " +
      "157367552 4982925 0 47077660 58061635\n"+
      "8      32 sdc 10148118 693255 %d 122125461 6090515 401630172 %d 2696685590 " +
      "0 26848216 2818793840\n"+
      "8      33 sdc1 10147917 693230 2054138426 122125426 6090506 401630172 " +
      "3261765880 2696685589 0 26848181 2818793804\n"+
      "8      64 sde 9989771 553047 %d 93407551 5978572 391997273 %d 2388274325 " +
      "0 24396646 2481664818\n"+
      "8      65 sde1 9989570 553022 1943973346 93407489 5978563 391997273 3183807264 " +
      "2388274325 0 24396584 2481666274\n"+
      "8      80 sdf 10197163 693995 %d 144374395 6216644 408395438 %d 2669389056 0 " +
      "26164759 2813746348\n"+
      "8      81 sdf1 10196962 693970 2033452794 144374355 6216635 408395438 3316897064 " +
      "2669389056 0 26164719 2813746308\n"+
      "8     129 sdi1 10078602 657936 2056552626 108362198 6134036 403851153 3279882064 " +
      "2639256086 0 26260432 2747601085\n";

  /**
   * Test parsing /proc/stat and /proc/cpuinfo
   * @throws IOException
   */
  @Test
  public void parsingProcStatAndCpuFile() throws IOException {
    // Write fake /proc/cpuinfo file.
    long numProcessors = 8;
    long cpuFrequencyKHz = 2392781;
    String fileContent = "";
    for (int i = 0; i < numProcessors; i++) {
      fileContent +=
          String.format(CPUINFO_FORMAT, i, cpuFrequencyKHz / 1000D, 0, 0)
              + "\n";
    }
    File tempFile = new File(FAKE_CPUFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_CPUFILE);
    fWriter.write(fileContent);
    fWriter.close();
    assertEquals(plugin.getNumProcessors(), numProcessors);
    assertEquals(plugin.getCpuFrequency(), cpuFrequencyKHz);

    // Write fake /proc/stat file.
    long uTime = 54972994;
    long nTime = 188860;
    long sTime = 19803373;
    tempFile = new File(FAKE_STATFILE);
    tempFile.deleteOnExit();
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCumulativeCpuTime(),
                 FAKE_JIFFY_LENGTH * (uTime + nTime + sTime));
    assertEquals(plugin.getCpuUsagePercentage(),
        (float)(CpuTimeTracker.UNAVAILABLE),0.0);
    assertEquals(plugin.getNumVCoresUsed(),
        (float)(CpuTimeTracker.UNAVAILABLE),0.0);

    // Advance the time and sample again to test the CPU usage calculation
    uTime += 100L;
    plugin.advanceTime(200L);
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCumulativeCpuTime(),
                 FAKE_JIFFY_LENGTH * (uTime + nTime + sTime));
    assertEquals(plugin.getCpuUsagePercentage(), 6.25F, 0.0);
    assertEquals(plugin.getNumVCoresUsed(), 0.5F, 0.0);

    // Advance the time and sample again. This time, we call getCpuUsagePercentage() only.
    uTime += 600L;
    plugin.advanceTime(300L);
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCpuUsagePercentage(), 25F, 0.0);
    assertEquals(plugin.getNumVCoresUsed(), 2F, 0.0);

    // Advance very short period of time (one jiffy length).
    // In this case, CPU usage should not be updated.
    uTime += 1L;
    plugin.advanceTime(1L);
    updateStatFile(uTime, nTime, sTime);
    assertEquals(plugin.getCumulativeCpuTime(),
                 FAKE_JIFFY_LENGTH * (uTime + nTime + sTime));
    assertEquals(
        plugin.getCpuUsagePercentage(), 25F, 0.0); // CPU usage is not updated.
    assertEquals(
        plugin.getNumVCoresUsed(), 2F, 0.0); // CPU usage is not updated.
  }

  /**
   * Write information to fake /proc/stat file
   */
  private void updateStatFile(long uTime, long nTime, long sTime)
    throws IOException {
    FileWriter fWriter = new FileWriter(FAKE_STATFILE);
    fWriter.write(String.format(STAT_FILE_FORMAT, uTime, nTime, sTime));
    fWriter.close();
  }

  /**
   * Test parsing /proc/meminfo
   * @throws IOException
   */
  @Test
  public void parsingProcMemFile() throws IOException {
    long memTotal = 4058864L;
    long memFree = 99632L;
    long inactive = 567732L;
    long swapTotal = 2096472L;
    long swapFree = 1818480L;
    int nrHugePages = 10;
    File tempFile = new File(FAKE_MEMFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_MEMFILE);
    fWriter.write(String.format(MEMINFO_FORMAT,
      memTotal, memFree, inactive, swapTotal, swapFree, nrHugePages));

    fWriter.close();
    assertEquals(plugin.getAvailablePhysicalMemorySize(),
                 1024L * (memFree + inactive));
    assertEquals(plugin.getAvailableVirtualMemorySize(),
                 1024L * (memFree + inactive + swapFree));
    assertEquals(plugin.getPhysicalMemorySize(),
        1024L * (memTotal - (nrHugePages * 2048)));
    assertEquals(plugin.getVirtualMemorySize(),
        1024L * (memTotal - (nrHugePages * 2048) + swapTotal));
  }

  /**
   * Test parsing /proc/meminfo with Inactive(file) present
   * @throws IOException
   */
  @Test
  public void parsingProcMemFile2() throws IOException {
    long memTotal = 131403836L;
    long memFree = 11257036L;
    long inactive = 27396032L;
    long inactiveFile = 21010696L;
    long swapTotal = 31981552L;
    long swapFree = 1818480L;
    long hardwareCorrupt = 31960904L;
    int nrHugePages = 10;
    File tempFile = new File(FAKE_MEMFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_MEMFILE);
    fWriter.write(String.format(MEMINFO_FORMAT_2,
      memTotal, memFree, inactive, inactiveFile, swapTotal, swapFree,
      hardwareCorrupt, nrHugePages));

    fWriter.close();
    assertEquals(plugin.getAvailablePhysicalMemorySize(),
                 1024L * (memFree + inactiveFile));
    assertFalse(plugin.getAvailablePhysicalMemorySize() ==
                 1024L * (memFree + inactive));
    assertEquals(plugin.getAvailableVirtualMemorySize(),
                 1024L * (memFree + inactiveFile + swapFree));
    assertEquals(plugin.getPhysicalMemorySize(),
                 1024L * (memTotal - hardwareCorrupt - (nrHugePages * 2048)));
    assertEquals(plugin.getVirtualMemorySize(),
                 1024L * (memTotal - hardwareCorrupt -
                          (nrHugePages * 2048) + swapTotal));
  }

  @Test
  public void testCoreCounts() throws IOException {

    String fileContent = "";
    // single core, hyper threading
    long numProcessors = 2;
    long cpuFrequencyKHz = 2392781;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, 0, 0));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(1, plugin.getNumCores());

    // single socket quad core, no hyper threading
    fileContent = "";
    numProcessors = 4;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, 0, i));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(4, plugin.getNumCores());

    // dual socket single core, hyper threading
    fileContent = "";
    numProcessors = 4;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, i / 2, 0));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(2, plugin.getNumCores());

    // dual socket, dual core, no hyper threading
    fileContent = "";
    numProcessors = 4;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, i / 2, i % 2));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(4, plugin.getNumCores());

    // dual socket, dual core, hyper threading
    fileContent = "";
    numProcessors = 8;
    for (int i = 0; i < numProcessors; i++) {
      fileContent =
          fileContent.concat(String.format(CPUINFO_FORMAT, i,
            cpuFrequencyKHz / 1000D, i / 4, (i % 4) / 2));
      fileContent = fileContent.concat("\n");
    }
    writeFakeCPUInfoFile(fileContent);
    plugin.setReadCpuInfoFile(false);
    assertEquals(numProcessors, plugin.getNumProcessors());
    assertEquals(4, plugin.getNumCores());
  }

  private void writeFakeCPUInfoFile(String content) throws IOException {
    File tempFile = new File(FAKE_CPUFILE);
    FileWriter fWriter = new FileWriter(FAKE_CPUFILE);
    tempFile.deleteOnExit();
    try {
      fWriter.write(content);
    } finally {
      IOUtils.closeQuietly(fWriter);
    }
  }

  /**
   * Test parsing /proc/net/dev
   * @throws IOException
   */
  @Test
  public void parsingProcNetFile() throws IOException {
    long numBytesReadIntf1 = 2097172468L;
    long numBytesWrittenIntf1 = 1355620114L;
    long numBytesReadIntf2 = 1097172460L;
    long numBytesWrittenIntf2 = 1055620110L;
    File tempFile = new File(FAKE_NETFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_NETFILE);
    fWriter.write(String.format(NETINFO_FORMAT,
                            numBytesReadIntf1, numBytesWrittenIntf1,
                            numBytesReadIntf2, numBytesWrittenIntf2));
    fWriter.close();
    assertEquals(plugin.getNetworkBytesRead(), numBytesReadIntf1 + numBytesReadIntf2);
    assertEquals(plugin.getNetworkBytesWritten(), numBytesWrittenIntf1 + numBytesWrittenIntf2);
  }

  /**
   * Test parsing /proc/diskstats
   * @throws IOException
   */
  @Test
  public void parsingProcDisksFile() throws IOException {
    long numSectorsReadsda = 1790549L; long numSectorsWrittensda = 1839071L;
    long numSectorsReadsdc = 20541402L; long numSectorsWrittensdc = 32617658L;
    long numSectorsReadsde = 19439751L; long numSectorsWrittensde = 31838072L;
    long numSectorsReadsdf = 20334546L; long numSectorsWrittensdf = 33168970L;
    File tempFile = new File(FAKE_DISKSFILE);
    tempFile.deleteOnExit();
    FileWriter fWriter = new FileWriter(FAKE_DISKSFILE);
    fWriter.write(String.format(DISKSINFO_FORMAT,
             numSectorsReadsda, numSectorsWrittensda,
             numSectorsReadsdc, numSectorsWrittensdc,
             numSectorsReadsde, numSectorsWrittensde,
             numSectorsReadsdf, numSectorsWrittensdf));

    fWriter.close();
    long expectedNumSectorsRead = numSectorsReadsda + numSectorsReadsdc +
                                  numSectorsReadsde + numSectorsReadsdf;
    long expectedNumSectorsWritten = numSectorsWrittensda + numSectorsWrittensdc +
                                     numSectorsWrittensde + numSectorsWrittensdf;
    // use non-default sector size
    int diskSectorSize = FakeLinuxResourceCalculatorPlugin.SECTORSIZE;
    assertEquals(expectedNumSectorsRead * diskSectorSize,
        plugin.getStorageBytesRead());
    assertEquals(expectedNumSectorsWritten * diskSectorSize,
        plugin.getStorageBytesWritten());
  }
}
