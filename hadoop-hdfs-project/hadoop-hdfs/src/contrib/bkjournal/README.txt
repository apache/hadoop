This module provides a BookKeeper backend for HFDS Namenode write
ahead logging.  

BookKeeper is a highly available distributed write ahead logging
system. For more details, see
   
    http://zookeeper.apache.org/bookkeeper

-------------------------------------------------------------------------------
How do I build?

 To generate the distribution packages for BK journal, do the
 following.

   $ mvn clean install -Pdist -Dtar

 This will generate a tarball, 
 target/hadoop-hdfs-bkjournal-<VERSION>.tar.gz 

-------------------------------------------------------------------------------
How do I use the BookKeeper Journal?

 To run a HDFS namenode using BookKeeper as a backend, extract the
 distribution package on top of hdfs

   cd hadoop-hdfs-<VERSION>/
   tar --strip-components 1 -zxvf path/to/hadoop-hdfs-bkjournal-<VERSION>.tar.gz

 Then, in hdfs-site.xml, set the following properties.

   <property>
     <name>dfs.namenode.edits.dir</name>
     <value>bookkeeper://localhost:2181/bkjournal,file:///path/for/edits</value>
   </property>

   <property>
     <name>dfs.namenode.edits.journal-plugin.bookkeeper</name>
     <value>org.apache.hadoop.contrib.bkjournal.BookKeeperJournalManager</value>
   </property>

 In this example, the namenode is configured to use 2 write ahead
 logging devices. One writes to BookKeeper and the other to a local
 file system. At the moment is is not possible to only write to 
 BookKeeper, as the resource checker explicitly checked for local
 disks currently.

 The given example, configures the namenode to look for the journal
 metadata at the path /bkjournal on the a standalone zookeeper ensemble
 at localhost:2181. To configure a multiple host zookeeper ensemble,
 separate the hosts with semicolons. For example, if you have 3
 zookeeper servers, zk1, zk2 & zk3, each listening on port 2181, you
 would specify this with 
  
   bookkeeper://zk1:2181;zk2:2181;zk3:2181/bkjournal

 The final part /bkjournal specifies the znode in zookeeper where
 ledger metadata will be store. Administrators can set this to anything
 they wish.


