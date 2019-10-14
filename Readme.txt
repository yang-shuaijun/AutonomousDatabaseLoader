Autonomous Database数据加载工具

1、下载快速驱动
从以下链接下载Oracle Autonomous Database 快速驱动
https://www.oracle.com/database/technologies/instant-client/downloads.html
解压缩快速驱动。

2、设置环境变量
ORACLE_HOME指向将解压路径。
将解压路径添加到PATH环境变量。

3、下载ADW/ATP wallet
将wallet解压到ORACLE_HOME指向的路劲内。
解压wallet。
更新sqlnet.ora内的WALLET_LOCATION，指向wallet的解压路径。

4、下载Python 3.7
https://www.python.org/downloads/windows/
然后安装相关软件包
pip install cx_oracle
pip install sqlalchemy
pip install pandas
pip install xlrd
pip install rsa