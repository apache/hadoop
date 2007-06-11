package org.apache.hadoop.util;

import java.io.*;
import java.util.Set;
import java.util.HashSet;


// Keeps track of which datanodes are allowed to connect to the namenode.
public class HostsFileReader {
  private Set<String> includes;
  private Set<String> excludes;
  private String includesFile;
  private String excludesFile;

  public HostsFileReader(String inFile, 
                         String exFile) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    includesFile = inFile;
    excludesFile = exFile;
    refresh();
  }

  private void readFileToSet(String filename, Set<String> set) throws IOException {
    FileInputStream fis = new FileInputStream(new File(filename));
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (int i = 0; i < nodes.length; i++) {
            set.add(nodes[i]);  // might need to add canonical name
          }
        }
      }   
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }  
  }

  public void refresh() throws IOException {
    includes.clear();
    excludes.clear();
    
    if (!includesFile.equals("")) {
      readFileToSet(includesFile, includes);
    }
    if (!excludesFile.equals("")) {
      readFileToSet(excludesFile, excludes);
    }
  }

  public Set<String> getHosts() {
    return includes;
  }

  public Set<String> getExcludedHosts() {
    return excludes;
  }

}
