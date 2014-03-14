tomcat_home_tail=ps
tomcat_home=/home/hadoop/catalina/apache.tomcat.ps
baseDir=`dirname $0`/..
aid=user_category_service
sh ${tomcat_home}/bin/shutdown.sh
sleep 2
proc=`ps aux | grep java | grep tomcat | grep -v "grep" | grep ${tomcat_home_tail} | awk '{print $2}'`
if [ "" != "$proc" ];then
  echo "Tomcat(${proc}) is not shutdown, and it will be killed directly."
  kill -9 $proc
fi
echo "Tomcat is shutdown."

rm -rf ${tomcat_home}/webapps/${aid}
rm -rf ${tomcat_home}/webapps/${aid}*.war
ServiceWar=$baseDir/target/${aid}.war
echo "cp $ServiceWar ${tomcat_home}/webapps/"
cp $ServiceWar ${tomcat_home}/webapps/
$tomcat_home/bin/startup.sh