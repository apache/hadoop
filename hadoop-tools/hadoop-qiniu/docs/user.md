# 使用七牛 Kodo 作为 Hadoop 兼容的文件系统

## 配置方法

### hadoop-env.sh 配置

打开文件$HADOOP_HOME/etc/hadoop/hadoop-env.sh，添加如下配置：

```shell
export HADOOP_OPTIONAL_TOOLS="hadoop-qiniu"
```

### core-site.xml 配置

修改`$HADOOP_HOME/etc/hadoop/core-site.xml`，增加Kodo相关的用户配置与实现类相关信息。

```xml

<configuration>
    <property>
        <name>fs.qiniu.download.blockSize</name>
        <value>4194304</value>
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
    </property>
    <property>
        <name>fs.AbstractFileSystem.kodo.impl</name>
        <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodo</value>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>kodo://example-bucket-name/</value>
    </property>
</configuration>

```

更多具体配置项说明与默认值可参考yml文件：[config.yml](config.yml)

可自行通过"."分隔符将yml分级描述的配置项转换为xml配置项，并补充命名空间前缀 fs.qiniu

如对于代理配置：

```yml
# 代理设置
proxy:
  enable: true
  hostname: '127.0.0.1'
  port: 8080
```

转换为xml配置项为：

```xml

<configuration>
    <property>
        <name>fs.qiniu.proxy.enable</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.qiniu.proxy.hostname</name>
        <value>127.0.0.1</value>
    </property>
    <property>
        <name>fs.qiniu.proxy.port</name>
        <value>8080</value>
    </property>
</configuration>
```

## 测试运行 mapreduce 示例程序 wordcount

### put命令

```shell
mkdir testDir
touch testDir/input.txt
echo "a b c d ee a b s" > testDir/input.txt
hadoop fs -put testDir kodo:///testDir

```

### ls命令

```shell
hadoop fs -ls -R kodo://example-bucket/
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user/root
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/testDir
-rw-rw-rw-   0 root root         17 2023-01-18 15:54 kodo://example-bucket/testDir/input.txt
```

### get命令

```shell
$ hadoop fs -get kodo:///testDir testDir1
$ ls -l -R testDir1
total 8
-rw-r--r--  1 root  staff  17 Jan 18 15:57 input.txt
```

### 运行 wordcount 示例

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

### cat命令

```text
$ hadoop fs -cat kodo://example-bucket/testDir/output/part-r-00000
a	2
b	2
c	1
d	1
ee	1
s	1
```