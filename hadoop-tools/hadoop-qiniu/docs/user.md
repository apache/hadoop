# 安装方法

## 获取依赖

需要两个依赖，分别如下

七牛的sdk的jar包: qiniu-java-sdk-{version}.jar

hadoop-qiniu接入的jar包: hadoop-qiniu-{version}.jar

### 手动编译hadoop-qiniu接入的jar包

在/hadoop-tools/hadoop-qiniu子模块下，执行如下命令开始jar包的编译

```sh
mvn package -Pdist -DskipTests -Dtar -Dmaven
```

在target文件夹下可获得对应jar包

### 直接下载hadoop-qiniu接入的jar包

[下载地址]()

### 直接下载qiniu-java-sdk包

[this latest JAR](https://search.maven.org/remote_content?g=com.qiniu&a=qiniu-java-sdk&v=LATEST)

## 安装依赖

将两个jar包拷贝到`$HADOOP_HOME/share/hadoop/tools/lib`目录下

修改`$HADOOP_HOME/etc/hadoop/hadoop_env.sh`文件，增加如下内容，将相关jar包加入Hadoop的CLASSPATH环境变量。

```shell
for f in $HADOOP_HOME/share/hadoop/tools/lib/*.jar; do
  if [ "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
  else
    export HADOOP_CLASSPATH=$f
  fi
done
```

# 使用方法

## Hadoop 配置

修改`$HADOOP/etc/hadoop/core-site.xml`，增加Kodo相关的用户配置与实现类相关信息。

```xml

<configuration>
    <property>
        <name>fs.qiniu.download.blockSize</name>
        <value>4194304</value>
        <description>分块下载大小</description>
    </property>

    <property>
        <name>fs.qiniu.auth.accessKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
    </property>

    <property>
        <name>fs.qiniu.auth.secretKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
    </property>
    
    <property>
        <name>fs.kodo.impl</name>
        <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem</value>
        <description>
            The implementation class of the Qiniu Kodo Filesystem.
        </description>
    </property>
    <property>
        <name>fs.AbstractFileSystem.kodo.impl</name>
        <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodo</value>
        <description>
            The implementation class of the Qiniu Kodo AbstractFileSystem.
        </description>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>kodo://example-bucket-name/</value>
    </property>

</configuration>

```

更多具体配置项说明与默认值可参考yml文件：[config.yml](config.yml)

可自行通过"."分隔符将yml分级描述的配置项转换为xml配置项

## put命令

```shell
mkdir testDir
touch testDir/input.txt
echo "a b c d ee a b s" > testDir/input.txt
hadoop fs -put testDir kodo:///testDir
```

## ls命令

```shell
hadoop fs -ls -R kodo://example-bucket/
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user/root
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/testDir
-rw-rw-rw-   0 root root         17 2023-01-18 15:54 kodo://example-bucket/testDir/input.txt
```

## get命令

```shell
$ hadoop fs -get kodo:///testDir testDir1
$ ls -l -R testDir1
total 8
-rw-r--r--  1 root  staff  17 Jan 18 15:57 input.txt
```

## 运行wordcount

```shell
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-{version}.jar wordcount kodo://example-bucket/testDir/input.txt kodo://example-bucket/testDir/output
```

执行成功后返回统计信息

```text
2023-01-18 16:00:49,228 INFO mapreduce.Job: Counters: 35
	File System Counters
		FILE: Number of bytes read=564062
		FILE: Number of bytes written=1899311
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		KODO: Number of bytes read=34
		KODO: Number of bytes written=25
		KODO: Number of read operations=3
		KODO: Number of large read operations=0
		KODO: Number of write operations=0
	Map-Reduce Framework
		Map input records=1
		Map output records=8
		Map output bytes=49
		Map output materialized bytes=55
		Input split bytes=102
		Combine input records=8
		Combine output records=6
		Reduce input groups=6
		Reduce shuffle bytes=55
		Reduce input records=6
		Reduce output records=6
		Spilled Records=12
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=31
		Total committed heap usage (bytes)=538968064
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=17
	File Output Format Counters 
		Bytes Written=25
```

```text
$ hadoop fs -ls -R kodo://example-bucket/
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user/root
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/testDir
-rw-rw-rw-   0 root root         17 2023-01-18 15:54 kodo://example-bucket/testDir/input.txt
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/testDir/output
-rw-rw-rw-   0 root root          0 2023-01-18 16:00 kodo://example-bucket/testDir/output/_SUCCESS
-rw-rw-rw-   0 root root         25 2023-01-18 16:00 kodo://example-bucket/testDir/output/part-r-00000
```

## cat命令

```text
$ hadoop fs -cat kodo://example-bucket/testDir/output/part-r-00000
a	2
b	2
c	1
d	1
ee	1
s	1
```