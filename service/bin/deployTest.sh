Tomcat_home=/home/hadoop/catalina/apache-tomcat-7.0.47.8182
baseDir=`dirname $0`/..
ServiceWar=$baseDir/target/user_category_service.war
echo "cp $ServiceWar $Tomcat_home/webapps/"
cp $ServiceWar $Tomcat_home/webapps/
$Tomcat_home/bin/shutdown.sh
$Tomcat_home/bin/startup.sh