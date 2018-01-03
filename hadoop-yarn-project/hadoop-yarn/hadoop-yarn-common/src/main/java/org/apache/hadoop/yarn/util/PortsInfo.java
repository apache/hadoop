package org.apache.hadoop.yarn.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ValueRanges;

public class PortsInfo {
  private static final Log LOG = LogFactory.getLog(PortsInfo.class);
  private long lastRefreshTime;
  static final int REFRESH_INTERVAL_MS = 2000;

  private ValueRanges ports;

  public PortsInfo() {
    lastRefreshTime = 0;
    reset();
  }

  long now() {
    return Time.monotonicNow();
  }

  void reset() {
    ports = null;
  }

  void refreshIfNeeded(boolean enableBitSet) {
    long now = now();
    if (now - lastRefreshTime > REFRESH_INTERVAL_MS) {
      lastRefreshTime = now;
      try {
        File f = new File("GetAllocatedPorts.ps1");
        if (!f.exists()) {
          Files.copy(
              PortsInfo.class.getResourceAsStream("/GetAllocatedPorts.ps1"),
              f.toPath());
        }
        // Use a ProcessBuilder
        ProcessBuilder pb =
            new ProcessBuilder("powershell.exe", f.getAbsolutePath());
        Process p = pb.start();
        InputStream is = p.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = null;
        String portsString = null;
        while ((line = br.readLine()) != null) {
          if (!line.isEmpty()) {
            portsString = line;
          }
        }
        if (portsString != null && !portsString.isEmpty()) {
          ports = ValueRanges.iniFromExpression(portsString, enableBitSet);
        } else {
          LOG.warn(
              "Get allocated ports result is empty, fail to get ports info ");
        }
        int r = p.waitFor(); // Let the process finish.
        // Remove it after finish
        f.deleteOnExit();
      } catch (Exception e) {
        LOG.warn("Fail to get allocated ports info ");
        e.printStackTrace();
      }
    }
  }

  public ValueRanges GetAllocatedPorts(boolean enableBitSet) {
    refreshIfNeeded(enableBitSet);
    return ports;
  }
}

