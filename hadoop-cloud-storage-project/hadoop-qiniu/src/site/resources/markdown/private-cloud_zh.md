# 私有云配置

对于私有云用户配置 Hadoop Qiniu 时，还需要在 `$HADOOP_HOME/etc/core-site.xml` 中添加或修改一些额外配置项：

```xml

<configuration>
    <property>
        <name>fs.qiniu.customRegion.id</name>
        <value>z0</value>
        <description>
            自定义Region的id，该id将用于后续域名配置的命名空间
        </description>
    </property>
    <property>
        <name>fs.qiniu.customRegion.custom.z0.ucServer</name>
        <value>https://uc.qiniuapi.com</value>
        <description>
            自定义Region的ucServer地址，若配置了该项，则会将自动获取后续域名配置，可无需手动配置
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.rsHost</name>
        <value>rs-z0.qiniuapi.com</value>
        <description>
            配置对象管理RS域名，注意不要添加http(s)://前缀，是否启用https请使用配置项fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.rsfHost</name>
        <value>rsf-z0.qiniuapi.com</value>
        <description>
            配置对象列举RSF域名，注意不要添加http(s)://前缀，是否启用https请使用配置项fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.apiHost</name>
        <value>api.qiniuapi.com</value>
        <description>
            配置API域名，注意不要添加http(s)://前缀，是否启用https请使用配置项fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.iovipHost</name>
        <value>iovip.qiniuio.com</value>
        <description>
            配置源站下载iovip域名，注意不要添加http(s)://前缀，是否启用https请使用配置项fs.qiniu.useHttps
        </description>
    </property>

    <property>
        <name>fs.qiniu.customRegion.custom.z0.srcUpHosts</name>
        <value>up.qiniup.com</value>
        <description>
            配置源站上传域名，注意不要添加http(s)://前缀，是否启用https请使用配置项fs.qiniu.useHttps
            若有多个域名配置，以英文逗号分隔
        </description>
    </property>
    <property>
        <name>fs.qiniu.customRegion.custom.z0.accUpHosts</name>
        <value>upload.qiniup.com</value>
        <description>
            配置加速上传域名，注意不要添加http(s)://前缀，是否启用https请使用配置项fs.qiniu.useHttps
            若有多个域名配置，以英文逗号分隔
        </description>
    </property>
    <property>
        <name>fs.qiniu.customRegion.custom.z0.ioSrcHost</name>
        <value>bucketname.kodo-cn-east-1.qiniucs.com</value>
        <description>
            配置默认源站域名，将用于下载文件，注意不要添加http(s)://前缀，
            是否使用https选项请在fs.qiniu.download.useHttps中配置，
            可选使用fs.qiniu.download.domain配置覆盖默认源站下载域名，
            覆盖后下载文件将不走该配置项的默认源站域名
        </description>
    </property>
    <property>
        <name>fs.qiniu.useHttps</name>
        <value>false</value>
        <description>
            配置上传，管理相关的域名是否使用https，默认为 true, 即使用 https
            私有云环境可能通常使用 http, 若有需要，可配置为 false，将使用 http
        </description>
    </property>
    <property>
        <name>fs.qiniu.download.useHttps</name>
        <value>false</value>
        <description>
            配置下载相关的域名是否使用https，默认为 true, 即使用 https
            私有云环境可能通常使用 http, 若有需要，可配置为 false，将使用 http
        </description>
    </property>
</configuration>
```