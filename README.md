## 说明

###背景

**此分支是为了让kylin支持多套集群环境****

1. 计算集群环境：kerberos认证，cdh安装
2. 存储hbase on hdfs：无kerberos认证，ambari安装

### 问题

kylin缺陷（主要集中在kylin-storage-hbase）

1. 代码设计没有考虑到计算集群和hbase集群不同的认证环境，UGI会直接出错
2. cube构建过程中，有本地MR/spark任务，也有提交到YARN集群的任务，在kerberos环境下，user并不是同一个。不同任务类型写到目标hbase hdfs时会出现权限问题
3. bulkload操作，理想化认为当前操作用户具有最高权限
4. cube merge后，清理hdfs废弃文件，存在安全问题

### 改动点

1. 计算集群和hbase单独集群的配置分离，**hbase的connection和hdfs Filesystem动态加载**，可以分别满足计算集群kbs认证和hbase无认证需要
2. 在createHtableJob后加上后置操作，保证cube构建过程中，生成hfile的yarn 任务具有写入权限
3. 简化Bulkload操作，移除不合理的chmod
4. 重构hdfs carbage回收，改为记录下garbage列表

### 可能存在的隐患

1. 本次改动点绝大多数在kylin-storage-hbase，相当于定制化适配Hbase，理论上无隐患
2. 后续kylin版本升级，是将更多的本地任务转到yarn上，有些操作的用户必然会等于hadoop用户，而不是本地UGI
   1. 后续版本升级，需要先看cube构建过程的改动，评估、测试后才可以去选择性升级
   2. 后续mr任务替换为spark任务，cube构建过程会有一些影响，需要评估



Apache Kylin
============

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/74f0139786cd4e8a8ce69bb0c17c2e71)](https://www.codacy.com/app/kyligence-git/kylin?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=apache/kylin&amp;utm_campaign=Badge_Grade)
[![Quality Gate](https://sonarcloud.io/api/badges/gate?key=org.apache.kylin%3Akylin)](https://sonarcloud.io/dashboard/index/org.apache.kylin%3Akylin)
[![SonarCloud Coverage](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=coverage)](https://sonarcloud.io/component_measures/metric/coverage/list?id=org.apache.kylin%3Akylin)
[![SonarCloud Bugs](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=bugs)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=org.apache.kylin%3Akylin)
[![SonarCloud Vulnerabilities](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=vulnerabilities)](https://sonarcloud.io/component_measures/metric/security_rating/list?id=org.apache.kylin%3Akylin)

> Extreme OLAP Engine for Big Data

Apache Kylin is an open source Distributed Analytics Engine, contributed by eBay Inc., provides SQL interface and multi-dimensional analysis (OLAP) on Hadoop supporting extremely large datasets.

For more details, see the website [http://kylin.apache.org](http://kylin.apache.org).

Documentation
=============
Please refer to [http://kylin.apache.org/docs21/](http://kylin.apache.org/docs21/).

Get Help
============
The fastest way to get response from our developers is to send email to our mail list <dev@kylin.apache.org>,   
and remember to subscribe our mail list via <dev-subscribe@kylin.apache.org>

License
============
Please refer to [LICENSE](https://github.com/apache/kylin/blob/master/LICENSE) file.





