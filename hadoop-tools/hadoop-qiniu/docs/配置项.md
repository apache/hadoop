# 鉴权

**(必填)** fs.qiniu.auth.accessKey

+ 类型: String
+ 描述: 七牛 accessKey

**(必填)** fs.qiniu.auth.secretKey

+ 类型: String
+ 描述: 七牛 secretKey

# 下载相关

fs.qiniu.download.blockSize

+ 类型: int
+ 描述: 下载文件时文件分块下载，文件块大小，这也是缓存块的最小单位，单位为字节
+ 默认值: 4 * 1024 * 1024B = 4MB

fs.qiniu.download.domain

+ 类型: String
+ 描述: 下载文件的域名，可自行配置cdn域名
+ 默认值: 默认走源站下载

fs.qiniu.download.cache.memory.blocks

+ 类型: int
+ 描述: 下载文件时的内存缓存中的最大块数，即最大内存占用 = 内存缓存最大块数x块大小
+ 默认值: 25块

fs.qiniu.download.cache.disk.enable

+ 类型: boolean
+ 描述: 是否启用磁盘缓存
+ 默认值: false，不使用磁盘缓存

fs.qiniu.download.cache.disk.blocks

+ 类型: int
+ 描述: 磁盘缓存最大块数，即磁盘最大占用=磁盘缓存最大块数*块大小
+ 默认值: 120块

fs.qiniu.download.cache.disk.dir

+ 类型: String
+ 描述: 磁盘缓存文件目录
+ 默认值: 默认使用 `hadoop.tmp.dir`配置项下的`qiniu`文件夹作为缓存文件夹

fs.qiniu.download.sign.enable

+ 类型: boolean
+ 描述: 是否启用下载签名
+ 默认值: 默认true，启用下载签名

fs.qiniu.download.sign.expires

+ 类型: int
+ 描述: 过期时间，单位为秒
+ 默认值: 604800, 即7 * 24 * 3600，七天

# 上传相关

fs,qiniu.upload.sign.expires

+ 类型: int
+ 描述: 上传签名的过期时间，单位为秒
+ 默认值: 604800, 即7 * 24 * 3600，七天

# Region相关

fs.qiniu.region.id

+ 类型: String
+ 描述: 设置bucket的regionId
+ 默认值: 可自动获取对应region

## 公有云 Region 配置

默认不填 regionId 的情况下也可自动获取 region 信息，显式地配置有利于加速文件系统的初始化，公有云的 regionId 可配置如下：

    华东: cn-east-1, z0
    
    华东-浙江: cn-east-2
    
    华北: cn-north-1, z1
    
    华南: cn-south-1, z2
    
    北美: us-north-1, na0
    
    新加坡: ap-southeast-1
    
    亚太-首尔: ap-northeast-1

## 私有云 Region 配置

若用户想部署在私有云上，可自行命名一个region id 并填写以下若干域名配置，其中`{customId}`使用自行命名的region id替代

fs.qiniu.region.custom.{customId}.rsHost

fs.qiniu.region.custom.{customId}.rsfHost

fs.qiniu.region.custom.{customId}.apiHost

fs.qiniu.region.custom.{customId}.iovipHost

fs.qiniu.region.custom.{customId}.accUpHosts

fs.qiniu.region.custom.{customId}.srcUpHosts

fs.qiniu.region.custom.{customId}.endpointHost

PS: Region的定位逻辑为：

首先优先将region id按照公有云进行Region的查找，若查找成功则返回，否则按照用户自行配置的私有云进行查找构造Region。

endpointHost为源站下载域名，若为公有云，则自动构造该字段

# 其他

fs.qiniu.useHttps

+ 类型: boolean
+ 描述: 是否使用https
+ 默认值: true