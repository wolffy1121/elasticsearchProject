# elasticsearchProject



易错点：


ERROR StatusLogger Log4j2 could not find a logging implementation

解决方案：
参考：
https://stackoverflow.com/questions/47881821/error-statuslogger-log4j2-could-not-find-a-logging-implementation

his dependency help to avoid this error from lambda.

<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-to-slf4j</artifactId>
    <version>2.8.2</version>
</dependency>
