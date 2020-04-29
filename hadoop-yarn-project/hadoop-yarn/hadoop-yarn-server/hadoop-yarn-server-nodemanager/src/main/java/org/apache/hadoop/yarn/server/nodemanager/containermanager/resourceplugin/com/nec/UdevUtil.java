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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nec;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

class UdevUtil {
  private static LibUdev libUdev;

  public synchronized void init() {
    LibUdev.init();
    libUdev = LibUdev.instance;
  }

  public String getSysPath(int deviceNo, char devType) {
    Pointer udev = null;
    Pointer device = null;

    try {
      udev = libUdev.udev_new();
      device = libUdev.udev_device_new_from_devnum(
          udev, (byte)devType, deviceNo);
      if (device == null) {
        throw new IllegalArgumentException("Udev: device not found");
      }
      Pointer sysPathPtr = libUdev.udev_device_get_syspath(device);
      if (sysPathPtr == null) {
        throw new IllegalArgumentException(
            "Udev: syspath not found for device");
      }
      return sysPathPtr.getString(0);
    } finally {
      if (device != null) {
        libUdev.udev_device_unref(device);
      }

      if (udev != null) {
        libUdev.udev_unref(udev);
      }
    }
  }

  @SuppressWarnings({"checkstyle:staticvariablename", "checkstyle:methodname",
      "checkstyle:parametername"})
  private static class LibUdev implements LibUdevMapping {
    private static LibUdev instance;

    public static void init() {
      if (instance == null) {
        Native.register("udev");
        instance = new LibUdev();
      }
    }

    public native Pointer udev_new();

    public native Pointer udev_unref(Pointer udev);

    public native Pointer udev_device_new_from_devnum(Pointer udev,
        byte type,
        int devnum);

    public native Pointer udev_device_get_syspath(Pointer udev_device);

    public native Pointer udev_device_unref(Pointer udev_device);
  }

  @SuppressWarnings({"checkstyle:staticvariablename", "checkstyle:methodname",
      "checkstyle:parametername"})
  interface LibUdevMapping {
    Pointer udev_new();

    Pointer udev_unref(Pointer udev);

    Pointer udev_device_new_from_devnum(Pointer udev,
        byte type,
        int devnum);

    Pointer udev_device_get_syspath(Pointer udev_device);

    Pointer udev_device_unref(Pointer udev_device);
  }
}
