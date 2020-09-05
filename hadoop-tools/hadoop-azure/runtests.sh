. ./testsupport.sh

scenario=
properties=("fs.azure.abfs.account.name" "fs.azure.test.namespace.enabled"
"fs.azure.account.auth.type")
values=("{accountname}.dfs.core.windows.net" "true" "OAuth")
runtestwithconfs

scenario=HNS-SharedKey
properties=("fs.azure.abfs.account.name" "fs.azure.test.namespace.enabled" "fs.azure.account.auth.type")
values=("{accountname}.dfs.core.windows.net" "true" "SharedKey")
runtestwithconfs

scenario=NonHNS-SharedKey
properties=("fs.azure.abfs.account.name" "fs.azure.test.namespace.enabled" "fs.azure.account.auth.type")
values=("{accountname}.dfs.core.windows.net" "false" "SharedKey")
runtestwithconfs
