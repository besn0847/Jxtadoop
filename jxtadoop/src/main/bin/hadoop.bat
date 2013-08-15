@ECHO OFF

@REM *** Set global variable
SET JRE_HOME="%JAVA_HOME"
SET JAVA="%JAVA_HOME\bin\java.exe"
SET JAVA_HEAP_MAX=-Xmx64M
SET HADOOP_DATANODE_OPTS=
SET HADOOP_CLASSPATH=
SET HADOOP_OPTS=

@REM *** Find the bin directory for the start-dfs.bat script
SET BIN=%~dp0 
CD %BIN%

@REM *** Find the Hadoop home directory
PUSHD ..
SET HADOOP_HOME=%CD%
SET PATH=%PATH%:%BIN%
POPD

@REM *** Set all the variables
:START
SET HADOOP_CONF_DIR=%HADOOP_HOME%\etc
IF NOT EXIST %HADOOP_CONF_DIR% GOTO :ERROR
SET HADOOP_ROOT_LOGGER="INFO,DRFA"
SET CLASSPATH=%HADOOP_HOME%;%HADOOP_CONF_DIR%
SET CLASSPATH=%CLASSPATH%;%JAVA_HOME%\lib\tools.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\bcprov-jdk15-140.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\commons-cli-1.2.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\commons-codec-1.3.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\commons-logging-1.0.4.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\commons-net-1.4.1.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\derby-10.5.3.0_1.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\h2-1.1.118.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\httptunnel-0.92.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\jets3t-0.6.1.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\jetty-4.2.12.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\jxse-2.7.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\kfs-0.3.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\log4j-1.2.16.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\netty-3.1.5.GA.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\resources.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\servlet-api-2.3.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\xmlenc-0.52.jar
SET CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\lib\jxtadoop-%{APP_VERSION}.jar

SET HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_DATANODE_OPTS%
SET HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.dir=%HADOOP_LOG_DIR%
SET HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.file=%HADOOP_LOGFILE%
SET HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.home.dir=%HADOOP_HOME%
SET HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.id.str=%HADOOP_IDENT_STRING%
SET HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.root.logger=%HADOOP_ROOT_LOGGER%
SET HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.policy.file=%HADOOP_POLICYFILE%
SET HADOOP_OPTS=%HADOOP_OPTS% -Djava.util.logging.config.file=%HADOOP_HOME%/etc/logging.properties
SET P2P_OPTS="-Dnet.jxta.impl.cm.Srdi.backend.impl=net.jxta.impl.cm.XIndiceSrdi"

@REM Process command line
IF "%1" == "archive" goto ARCHIVE
IF "%1" == "daemonlog" goto LOG
IF "%1" == "dfsadmin" goto DFSADMIN
IF "%1" == "distcp" goto DISTCP
IF "%1" == "fsck" goto FSCK
IF "%1" == "fs" goto FS
IF "%1" == "jar" goto JAR
IF "%1" == "p2p" goto P2P
IF "%1" == "version" goto VERSION
GOTO USAGE

:ARCHIVE
SET CLASS=org.apache.jxtadoop.tools.HadoopArchives
GOTO RUN

:DFSADMIN
SET CLASS=org.apache.jxtadoop.hdfs.tools.DFSAdmin
GOTO RUN

:DISTCP
SET CLASS=org.apache.jxtadoop.tools.DistCp
GOTO RUN

:FSCK
SET CLASS=org.apache.jxtadoop.hdfs.tools.DFSck
GOTO RUN

:FS
SET CLASS=org.apache.jxtadoop.fs.FsShell
GOTO RUN

:JAR
SET CLASS=org.apache.jxtadoop.util.RunJar
GOTO RUN

:LOG
SET CLASS=org.apache.jxtadoop.log.LogLevel
GOTO RUN

:P2P
SET CLASS=org.apache.jxtadoop.hdfs.p2p.P2PShell
GOTO RUN

:VERSION
SET CLASS=org.apache.jxtadoop.util.VersionInfo
GOTO RUN

:JAR
SET 
GOTO RUN

:RUN
%JAVA% %JAVA_HEAP_MAX% %P2P_OPTS% %HADOOP_OPTS% -classpath "%CLASSPATH%" %CLASS% %2 %3 %4 %5 %6 %7 %8 %9
GOTO END

:USAGE
ECHO "Usage: hadoop COMMAND"
ECHO "  archive -archiveName NAME <src>* <dest> create a hadoop archive"
ECHO "  daemonlog            get/set the log level for each daemon"
ECHO "  dfsadmin             run a DFS admin client"
ECHO "  distcp <srcurl> <desturl> copy file or directories recursively"
ECHO "  fsck                 run a DFS filesystem checking utility"
ECHO "  fs                   run a generic filesystem user client"
ECHO "  jar <jar>            run a jar file"
ECHO "  p2p                  run a peer-to-peer network client"
ECHO "  version              print the version"
GOTO END

:ERROR
ECHO Cannot run Hadoop command
GOTO END

:END