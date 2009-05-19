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

package org.apache.hadoop.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeSet;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

/** An implementation of UserGroupInformation in the Unix system */
public class UnixUserGroupInformation extends UserGroupInformation {
  public static final String DEFAULT_USERNAME = "DrWho";
  public static final String DEFAULT_GROUP = "Tardis";

  final static public String UGI_PROPERTY_NAME = "hadoop.job.ugi";
  final static private HashMap<String, UnixUserGroupInformation> user2UGIMap =
    new HashMap<String, UnixUserGroupInformation>();

  /** Create an immutable {@link UnixUserGroupInformation} object. */
  public static UnixUserGroupInformation createImmutable(String[] ugi) {
    return new UnixUserGroupInformation(ugi) {
      public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }

  private String userName;
  private String[] groupNames;

  /** Default constructor
   */
  public UnixUserGroupInformation() {
  }

  /** Constructor with parameters user name and its group names.
   * The first entry in the groups list is the default  group.
   * 
   * @param userName a user's name
   * @param groupNames groups list, first of which is the default group
   * @exception IllegalArgumentException if any argument is null
   */
  public UnixUserGroupInformation(String userName, String[] groupNames) {
    setUserGroupNames(userName, groupNames);
  }

  /** Constructor with parameter user/group names
   * 
   * @param ugi an array containing user/group names, the first
   *                     element of which is the user name, the second of
   *                     which is the default group name.
   * @exception IllegalArgumentException if the array size is less than 2 
   *                                     or any element is null.
   */
  public UnixUserGroupInformation(String[] ugi) {
    if (ugi==null || ugi.length < 2) {
      throw new IllegalArgumentException( "Parameter does contain at least "+
          "one user name and one group name");
    }
    String[] groupNames = new String[ugi.length-1];
    System.arraycopy(ugi, 1, groupNames, 0, groupNames.length);
    setUserGroupNames(ugi[0], groupNames);
  }
  
  /* Set this object's user name and group names
   * 
   * @param userName a user's name
   * @param groupNames groups list, the first of which is the default group
   * @exception IllegalArgumentException if any argument is null
   */
  private void setUserGroupNames(String userName, String[] groupNames) {
    if (userName==null || userName.length()==0 ||
        groupNames== null || groupNames.length==0) {
      throw new IllegalArgumentException(
          "Parameters should not be null or an empty string/array");
    }
    for (int i=0; i<groupNames.length; i++) {
      if(groupNames[i] == null || groupNames[i].length() == 0) {
        throw new IllegalArgumentException("A null group name at index " + i);
      }
    }
    this.userName = userName;
    this.groupNames = groupNames;
  }

  /** Return an array of group names
   */
  public String[] getGroupNames() {
    return groupNames;
  }

  /** Return the user's name
   */
  public String getUserName() {
    return userName;
  }

  /* The following two methods implements Writable interface */
  final private static String UGI_TECHNOLOGY = "STRING_UGI"; 
  /** Deserialize this object
   * First check if this is a UGI in the string format.
   * If no, throw an IOException; otherwise
   * set this object's fields by reading them from the given data input
   *  
   *  @param in input stream
   *  @exception IOException is thrown if encounter any error when reading
   */
  public void readFields(DataInput in) throws IOException {
    // read UGI type first
    String ugiType = Text.readString(in);
    if (!UGI_TECHNOLOGY.equals(ugiType)) {
      throw new IOException("Expect UGI prefix: " + UGI_TECHNOLOGY +
          ", but receive a prefix: " + ugiType);
    }
    
    // read this object
    userName = Text.readString(in);
    int numOfGroups = WritableUtils.readVInt(in);
    groupNames = new String[numOfGroups];
    for (int i = 0; i < numOfGroups; i++) {
      groupNames[i] = Text.readString(in);
    }
  }

  /** Serialize this object
   * First write a string marking that this is a UGI in the string format,
   * then write this object's serialized form to the given data output
   * 
   * @param out output stream
   * @exception IOException if encounter any error during writing
   */
  public void write(DataOutput out) throws IOException {
    // write a prefix indicating the type of UGI being written
    Text.writeString(out, UGI_TECHNOLOGY);
    // write this object
    Text.writeString(out, userName);
    WritableUtils.writeVInt(out, groupNames.length);
    for (String groupName : groupNames) {
      Text.writeString(out, groupName);
    }
  }

  /* The following two methods deal with transferring UGI through conf. 
   * In this pass of implementation we store UGI as a string in conf. 
   * Later we may change it to be a more general approach that stores 
   * it as a byte array */
  /** Store the given <code>ugi</code> as a comma separated string in
   * <code>conf</code> as a property <code>attr</code>
   * 
   * The String starts with the user name followed by the default group names,
   * and other group names.
   * 
   * @param conf configuration
   * @param attr property name
   * @param ugi a UnixUserGroupInformation
   */
  public static void saveToConf(Configuration conf, String attr, 
      UnixUserGroupInformation ugi ) {
    conf.set(attr, ugi.toString());
  }
  
  /** Read a UGI from the given <code>conf</code>
   * 
   * The object is expected to store with the property name <code>attr</code>
   * as a comma separated string that starts
   * with the user name followed by group names.
   * If the property name is not defined, return null.
   * It's assumed that there is only one UGI per user. If this user already
   * has a UGI in the ugi map, return the ugi in the map.
   * Otherwise, construct a UGI from the configuration, store it in the
   * ugi map and return it.
   * 
   * @param conf configuration
   * @param attr property name
   * @return a UnixUGI
   * @throws LoginException if the stored string is ill-formatted.
   */
  public static UnixUserGroupInformation readFromConf(
      Configuration conf, String attr) throws LoginException {
    String[] ugi = conf.getStrings(attr);
    if(ugi == null) {
      return null;
    }
    UnixUserGroupInformation currentUGI = null;
    if (ugi.length>0 ){
      currentUGI = user2UGIMap.get(ugi[0]);
    }
    if (currentUGI == null) {
      try {
        currentUGI = new UnixUserGroupInformation(ugi);
        user2UGIMap.put(currentUGI.getUserName(), currentUGI);
      } catch (IllegalArgumentException e) {
        throw new LoginException("Login failed: "+e.getMessage());
      }
    }
    
    return currentUGI;
  }
  
  /**
   * Get current user's name and the names of all its groups from Unix.
   * It's assumed that there is only one UGI per user. If this user already
   * has a UGI in the ugi map, return the ugi in the map.
   * Otherwise get the current user's information from Unix, store it
   * in the map, and return it.
   *
   * If the current user's UNIX username or groups are configured in such a way
   * to throw an Exception, for example if the user uses LDAP, then this method
   * will use a the {@link #DEFAULT_USERNAME} and {@link #DEFAULT_GROUP}
   * constants.
   */
  public static UnixUserGroupInformation login() throws LoginException {
    try {
      String userName;

      // if an exception occurs, then uses the
      // default user
      try {
        userName =  getUnixUserName();
      } catch (Exception e) {
        userName = DEFAULT_USERNAME;
      }

      // check if this user already has a UGI object in the ugi map
      UnixUserGroupInformation ugi = user2UGIMap.get(userName);
      if (ugi != null) {
        return ugi;
      }

      /* get groups list from UNIX. 
       * It's assumed that the first group is the default group.
       */
      String[]  groupNames;

      // if an exception occurs, then uses the
      // default group
      try {
        groupNames = getUnixGroups();
      } catch (Exception e) {
        groupNames = new String[1];
        groupNames[0] = DEFAULT_GROUP;
      }

      // construct a Unix UGI
      ugi = new UnixUserGroupInformation(userName, groupNames);
      user2UGIMap.put(ugi.getUserName(), ugi);
      return ugi;
    } catch (Exception e) {
      throw new LoginException("Login failed: "+e.getMessage());
    }
  }

  /** Equivalent to login(conf, false). */
  public static UnixUserGroupInformation login(Configuration conf)
    throws LoginException {
    return login(conf, false);
  }
  
  /** Get a user's name & its group names from the given configuration; 
   * If it is not defined in the configuration, get the current user's
   * information from Unix.
   * If the user has a UGI in the ugi map, return the one in
   * the UGI map.
   * 
   *  @param conf either a job configuration or client's configuration
   *  @param save saving it to conf?
   *  @return UnixUserGroupInformation a user/group information
   *  @exception LoginException if not able to get the user/group information
   */
  public static UnixUserGroupInformation login(Configuration conf, boolean save
      ) throws LoginException {
    UnixUserGroupInformation ugi = readFromConf(conf, UGI_PROPERTY_NAME);
    if (ugi == null) {
      ugi = login();
      LOG.debug("Unix Login: " + ugi);
      if (save) {
        saveToConf(conf, UGI_PROPERTY_NAME, ugi);
      }
    }
    return ugi;
  }
  
  /* Return a string representation of a string array.
   * Two strings are separated by a blank.
   */
  private static String toString(String[] strArray) {
    if (strArray==null || strArray.length==0) {
      return "";
    }
    StringBuilder buf = new StringBuilder(strArray[0]);
    for (int i=1; i<strArray.length; i++) {
      buf.append(' ');
      buf.append(strArray[i]);
    }
    return buf.toString();
  }
  
  /** Get current user's name from Unix by running the command whoami.
   * 
   * @return current user's name
   * @throws IOException if encounter any error while running the command
   */
  static String getUnixUserName() throws IOException {
    String[] result = executeShellCommand(
        new String[]{Shell.USER_NAME_COMMAND});
    if (result.length!=1) {
      throw new IOException("Expect one token as the result of " + 
          Shell.USER_NAME_COMMAND + ": " + toString(result));
    }
    return result[0];
  }

  /** Get the current user's group list from Unix by running the command groups
   * 
   * @return the groups list that the current user belongs to
   * @throws IOException if encounter any error when running the command
   */
  private static String[] getUnixGroups() throws IOException {
    return executeShellCommand(Shell.getGROUPS_COMMAND());
  }
  
  /* Execute a command and return the result as an array of Strings */
  private static String[] executeShellCommand(String[] command)
  throws IOException {
    String groups = Shell.execCommand(command);
    StringTokenizer tokenizer = new StringTokenizer(groups);
    int numOfTokens = tokenizer.countTokens();
    String[] tokens = new String[numOfTokens];
    for (int i=0; tokenizer.hasMoreTokens(); i++) {
      tokens[i] = tokenizer.nextToken();
    }

    return tokens;
  }

  /** Decide if two UGIs are the same
   *
   * @param other other object
   * @return true if they are the same; false otherwise.
   */
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    
    if (!(other instanceof UnixUserGroupInformation)) {
      return false;
    }
    
    UnixUserGroupInformation otherUGI = (UnixUserGroupInformation)other;
    
    // check userName
    if (userName == null) {
      if (otherUGI.getUserName() != null) {
        return false;
      }
    } else {
      if (!userName.equals(otherUGI.getUserName())) {
        return false;
      }
    }
    
    // checkGroupNames
    if (groupNames == otherUGI.groupNames) {
      return true;
    }
    if (groupNames.length != otherUGI.groupNames.length) {
      return false;
    }
    // check default group name
    if (groupNames.length>0 && !groupNames[0].equals(otherUGI.groupNames[0])) {
      return false;
    }
    // check all group names, ignoring the order
    return new TreeSet<String>(Arrays.asList(groupNames)).equals(
           new TreeSet<String>(Arrays.asList(otherUGI.groupNames)));
  }

  /** Returns a hash code for this UGI. 
   * The hash code for a UGI is the hash code of its user name string.
   * 
   * @return  a hash code value for this UGI.
   */
  public int hashCode() {
    return getUserName().hashCode();
  }
  
  /** Convert this object to a string
   * 
   * @return a comma separated string containing the user name and group names
   */
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(userName);
    for (String groupName : groupNames) {
      buf.append(',');
      buf.append(groupName);
    }
    return buf.toString();
  }

  @Override
  public String getName() {
    return toString();
  }
}
