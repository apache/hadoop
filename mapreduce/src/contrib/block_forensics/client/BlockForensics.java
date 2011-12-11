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
 
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.Runtime;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This class repeatedly queries a namenode looking for corrupt replicas. If 
 * any are found a provided hadoop job is launched and the output printed
 * to stdout. 
 *
 * The syntax is:
 *
 * java BlockForensics http://[namenode]:[port]/corrupt_replicas_xml.jsp 
 *                    [sleep time between namenode query for corrupt blocks
 *                      (in seconds)] [mapred jar location] [hdfs input path]
 *
 * All arguments are required.
 */
public class BlockForensics {
  
  public static String join(List<?> l, String sep) {
    StringBuilder sb = new StringBuilder();
    Iterator it = l.iterator();
    
    while(it.hasNext()){
      sb.append(it.next());
      if (it.hasNext()) {
        sb.append(sep);
      }
    }
    
    return sb.toString();
  }
  
  
  // runs hadoop command and prints output to stdout
  public static void runHadoopCmd(String ... args)
  throws IOException {
    String hadoop_home = System.getenv("HADOOP_HOME");
    
    List<String> l = new LinkedList<String>();
    l.add("bin/hadoop");
    l.addAll(Arrays.asList(args));
    
    ProcessBuilder pb = new ProcessBuilder(l);
    
    if (hadoop_home != null) {
      pb.directory(new File(hadoop_home));
    }

    pb.redirectErrorStream(true);
          
    Process p = pb.start();

    BufferedReader br = new BufferedReader(
                          new InputStreamReader(p.getInputStream()));
    String line;

    while ((line = br.readLine()) != null) {
      System.out.println(line);
    }


  }
    
  public static void main(String[] args)
    throws SAXException, ParserConfigurationException, 
           InterruptedException, IOException {

    if (System.getenv("HADOOP_HOME") == null) {
      System.err.println("The environmental variable HADOOP_HOME is undefined");
      System.exit(1);
    }


    if (args.length < 4) {
      System.out.println("Usage: java BlockForensics [http://namenode:port/"
                         + "corrupt_replicas_xml.jsp] [sleep time between "
                         + "requests (in milliseconds)] [mapred jar location] "
                         + "[hdfs input path]");
      return;
    }
             
    int sleepTime = 30000;
  
    try {
      sleepTime = Integer.parseInt(args[1]);
    } catch (NumberFormatException e) {
      System.out.println("The sleep time entered is invalid, "
                         + "using default value: "+sleepTime+"ms"); 
    }
      
    Set<Long> blockIds = new TreeSet<Long>();
    
    while (true) {
      InputStream xml = new URL(args[0]).openConnection().getInputStream();
    
      DocumentBuilderFactory fact = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = fact.newDocumentBuilder();
      Document doc = builder.parse(xml);
         
      NodeList corruptReplicaNodes = doc.getElementsByTagName("block_id");

      List<Long> searchBlockIds = new LinkedList<Long>();
      for(int i=0; i<corruptReplicaNodes.getLength(); i++) {
        Long blockId = new Long(corruptReplicaNodes.item(i)
                                                    .getFirstChild()
                                                    .getNodeValue());
        if (!blockIds.contains(blockId)) {
          blockIds.add(blockId);
          searchBlockIds.add(blockId);
        }
      }
      
      if (searchBlockIds.size() > 0) {
        String blockIdsStr = BlockForensics.join(searchBlockIds, ",");
        System.out.println("\nSearching for: " + blockIdsStr);
        String tmpDir =
            new String("/tmp-block-forensics-" +
                Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        System.out.println("Using temporary dir: "+tmpDir);

        // delete tmp dir
        BlockForensics.runHadoopCmd("fs", "-rmr", tmpDir);
      
        // launch mapred job      
        BlockForensics.runHadoopCmd("jar",
                                    args[2], // jar location
                                    args[3], // input dir
                                    tmpDir, // output dir
                                    blockIdsStr// comma delimited list of blocks
                                    );
        // cat output
        BlockForensics.runHadoopCmd("fs", "-cat", tmpDir+"/part*");

        // delete temp dir
        BlockForensics.runHadoopCmd("fs", "-rmr", tmpDir);

        int sleepSecs = (int)(sleepTime/1000.);
        System.out.print("Sleeping for "+sleepSecs
                         + " second"+(sleepSecs == 1?"":"s")+".");
      }

      System.out.print(".");
      Thread.sleep(sleepTime);

    }
  }
}
