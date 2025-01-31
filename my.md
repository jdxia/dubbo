[TOC]

# 源码编译

```shell
rm -rf ~/.m2/repository/org/apache/dubbo

# 使用jdk8编译
mvn clean compile -DskipTests -Dmaven.javadoc.skip=true --settings ~/.m2/settings.xml.aliyun

```

# 源码资料

1. 代码架构
https://cn.dubbo.apache.org/zh-cn/overview/mannual/java-sdk/reference-manual/architecture/code-architecture/

2. 官方博客
> 3.0.8:  https://cn.dubbo.apache.org/zh-cn/blog/java/codeanalysis/3.0.8/

3. dubbo的SPI
> https://cn.dubbo.apache.org/zh-cn/overview/mannual/java-sdk/reference-manual/spi/description/dubbo-spi/



