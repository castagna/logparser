  Log Parser
  ==========

  Some quick and dirty code for parsing Apache logs ...


  Requirements
  ------------

  The only requirements are a Java JDK 1.6 and Apache Maven.

  Instructions on how to install Maven are here:
  http://maven.apache.org/download.html#Installation 


  How to run it
  -------------

  For example:

  mvn hadoop:pack
  hadoop --config ./conf/hadoop/ jar target/hadoop-deploy/logparser-hdeploy.jar com.cloudera.castagna.logparser.mr.StatusCodesStats -D overrideOutput=true input output

  ...


