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

package org.apache.hadoop.eclipse.servers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.server.HadoopServer;

/**
 * Registry for storing Hadoop Servers
 */
public class ServerRegistry {

  private static final ServerRegistry INSTANCE = new ServerRegistry();

  public static final int SERVER_ADDED = 0;

  public static final int SERVER_REMOVED = 1;

  public static final int SERVER_STATE_CHANGED = 2;

  private ServerRegistry() {
  }

  private List<HadoopServer> servers;

  private Set<IHadoopServerListener> listeners =
      new HashSet<IHadoopServerListener>();

  public static ServerRegistry getInstance() {
    return INSTANCE;
  }

  public List<HadoopServer> getServers() {
    return Collections.unmodifiableList(getServersInternal());
  }

  /**
   * Returns the list of currently defined servers. The list is read from the
   * file if it is not in memory.
   * 
   * @return the list of hadoop servers
   */
  private List<HadoopServer> getServersInternal() {

    if (servers == null) {
      servers = new ArrayList<HadoopServer>();

      File store =
          Activator.getDefault().getStateLocation().append("SERVERS.txt")
              .toFile();

      if (!store.exists()) {
        try {
          store.createNewFile();
        } catch (IOException e) {
          // pretty fatal error here - we cant save or restore
          throw new RuntimeException(e);
        }
      }

      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new FileReader(store));
        String line;
        while ((line = reader.readLine()) != null) {
          try {
            String[] parts = line.split("\t");
            if (parts.length == 1) {
              String location = parts[0];
              parts = new String[] { location, "Hadoop Server" };
            }

            if (parts.length > 2) {
              servers.add(new HadoopServer(parts[0], parts[1], parts[2],
                  parts[3]));
            } else {
              servers.add(new HadoopServer(parts[0], parts[1]));
            }

            servers.get(servers.size() - 1).setId(servers.size() - 1);

          } catch (Exception e) {
            // TODO(jz) show message and ignore - still want rest of
            // servers if we can get them
            e.printStackTrace();
          }
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        // TODO(jz) show message and ignore - may have corrupt
        // configuration
        e.printStackTrace();
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            /* nothing we can do */
          }
        }
      }
    }

    return servers;
  }

  public synchronized void removeServer(HadoopServer server) {
    getServersInternal().remove(server);
    fireListeners(server, SERVER_REMOVED);
    save();
  }

  public synchronized void addServer(HadoopServer server) {
    getServersInternal().add(server);
    fireListeners(server, SERVER_ADDED);
    save();
  }

  /**
   * Save the list of servers to the plug-in configuration file, currently
   * SERVERS.txt in
   * <workspace-dir>/.metadata/.plugins/org.apache.hadoop.eclipse/SERVERS.txt
   */
  private synchronized void save() {
    File store =
        Activator.getDefault().getStateLocation().append("SERVERS.txt")
            .toFile();
    BufferedWriter writer = null;

    if (!store.exists()) {
      try {
        store.createNewFile();
      } catch (IOException e) {
        // pretty fatal error here - we can't save or restore
        throw new RuntimeException(e);
      }
    }

    try {
      writer = new BufferedWriter(new FileWriter(store));
      int i = 0;
      for (HadoopServer server : servers) {
        server.setId(i++);
        writer.append(server.toString() + "\t" + server.getName());
        if (server.getTunnelHostName() != null) {
          writer.append("\t" + server.getTunnelHostName() + "\t"
              + server.getTunnelUserName());
        }
        writer.newLine();
      }
    } catch (IOException e) {
      // TODO(jz) show error message
      e.printStackTrace();
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          /* nothing we can do */
        }
      }
    }
  }

  public void addListener(IHadoopServerListener l) {
    synchronized (listeners) {
      listeners.add(l);
    }
  }

  private void fireListeners(HadoopServer location, int kind) {
    synchronized (listeners) {
      for (IHadoopServerListener listener : listeners) {
        listener.serverChanged(location, kind);
      }
    }
  }

  public void stateChanged(HadoopServer job) {
    fireListeners(job, SERVER_STATE_CHANGED);
  }

  public void removeListener(IHadoopServerListener l) {
    synchronized (listeners) {
      listeners.remove(l);
    }
  }

  public void dispose() {
    for (HadoopServer server : getServers()) {
      server.dispose();
    }
  }

  public HadoopServer getServer(int serverid) {
    return servers.get(serverid);
  }

}
