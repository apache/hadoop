# 离线测试说明

本项目还支持离线测试，离线测试基于内存中的 `HashMap` 数据结构模拟了 Kodo 服务，不需要任何认证信息。

但是，为了使得测试不被 `maven`
跳过，还是需要创建一个 `hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/auth-keys.xml`
文件，但不需要任何认证信息，可以创建文件内容如下：

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
</configuration>
```

还需要创建文件 `hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/contract-test-options.xml`，这个文件内容如下：

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.qiniu.test.useMock</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.contract.test.fs.mockkodo</name>
    <value>mockkodo://hadoop-java</value>
  </property>

  <property>
    <name>fs.mockkodo.impl</name>
    <value>org.apache.hadoop.fs.qiniu.kodo.MockQiniuKodoFileSystem</value>
  </property>

  <property>
    <name>fs.AbstractFileSystem.mockkodo.impl</name>
    <value>org.apache.hadoop.fs.qiniu.kodo.MockQiniuKodo</value>
  </property>
</configuration>
```

## 使用 maven 测试命令

为了运行契约测试，需要在`hadoop-cloud-storage-project/hadoop-qiniu/`文件夹下，使用`mvn`命令，通过使用`-Dtest`
参数指定所有需要运行的测试类，例如：

```shell
cd hadoop-cloud-storage-project/hadoop-qiniu/
mvn test -Dtest=ITestQiniuKodoFileSystemContractBase,ITestQiniuKodoContractCreate,ITestQiniuKodoContractDelete,ITestQiniuKodoContractDistCp,ITestQiniuKodoContractGetFileStatus,ITestQiniuKodoContractMkdir,ITestQiniuKodoContractOpen,ITestQiniuKodoContractRename,ITestQiniuKodoContractRootDir,ITestQiniuKodoContractSeek
```

> PS: 如果在运行`ITestQiniuKodoContractDistCp`测试类时，部分测试用例报错`java.lang.NoSuchMethodError`
> ，请先在最外层`hadoop`仓库中，执行`mvn install -DskipTests`命令

# 在线测试说明

为了测试 `kodo://` 文件系统，需要确保 `hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources` 中存在以下两个配置文件：

1. `auth-keys.xml`

2. `core-site.xml`

## `core-site.xml`

该文件已经存在，在大多数情况下，不需要修改该文件，除非特殊情况下需要在测试时设置一些非默认属性

## `auth-keys.xml`

这个文件将触发 Qiniu Kodo 模块测试，如果没有这个文件，这个模块将不会执行任何测试

它包含了`access key`和`secret key`以及一些可选一些代理配置以连接至Qiniu Kodo, 如果是私有云环境测试，还应当配置region的相关域名

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
        <name>fs.qiniu.auth.accessKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
    </property>
    <property>
        <name>fs.qiniu.auth.secretKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
    </property>
</configuration>
```

## 运行 Hadoop 契约测试

创建`hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/contract-test-options.xml`
文件，这个文件中的`fs.contract.test.fs.kodo`
定义了测试环境所用的文件系统路径，如果该属性未定义，则自动跳过这些契约测试。注意运行这些测试需要认证信息，这些认证信息将放入
`hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/auth-keys.xml` 文件中。并通过`XInclude`
标签包含进来，这是一个`contract-test-options.xml`的例子：

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <include xmlns="http://www.w3.org/2001/XInclude"
             href="auth-keys.xml"/>

    <property>
        <name>fs.qiniu.useHttps</name>
        <value>false</value>
    </property>

    <property>
        <name>fs.qiniu.download.useHttps</name>
        <value>false</value>
    </property>

    <property>
        <name>fs.qiniu.test.useMock</name>
        <value>false</value>
    </property>

    <property>
        <name>fs.contract.test.fs.kodo</name>
        <value>kodo://your-test-bucket</value>
    </property>

    <property>
        <name>fs.kodo.impl</name>
        <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem</value>
    </property>

    <property>
        <name>fs.qiniu.download.useNoCacheHeader</name>
        <value>true</value>
    </property>
</configuration>
```

### 使用maven测试命令

为了运行契约测试，需要在`hadoop-cloud-storage-project/hadoop-qiniu/`文件夹下，使用`mvn`命令，通过使用`-Dtest`
参数指定所有需要运行的测试类，例如：

```shell
cd hadoop-cloud-storage-project/hadoop-qiniu/
mvn test -Dtest=ITestQiniuKodoFileSystemContractBase,ITestQiniuKodoContractCreate,ITestQiniuKodoContractDelete,ITestQiniuKodoContractDistCp,ITestQiniuKodoContractGetFileStatus,ITestQiniuKodoContractMkdir,ITestQiniuKodoContractOpen,ITestQiniuKodoContractRename,ITestQiniuKodoContractRootDir,ITestQiniuKodoContractSeek
```

> PS: 如果在运行`ITestQiniuKodoContractDistCp`测试类时，部分测试用例报错`java.lang.NoSuchMethodError`
> ，请先在最外层`hadoop`仓库中，执行`mvn install -DskipTests`命令
