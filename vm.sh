#echo "Trying to set the Linux port configuration"
#sudo echo "1" >/proc/sys/net/ipv4/tcp_tw_reuse
echo "Compiling MJWordCount Application"
javac src/main/java/edu/illinois/cs/cs425/MJWordCount.java
echo "Copying MJWordCount Executable File and Cleaning Up"
cp src/main/java/edu/illinois/cs/cs425/MJWordCount.class MJWordCount.class
cp src/main/java/edu/illinois/cs/cs425/MJWordCountThread.class MJWordCountThread.class
rm src/main/java/edu/illinois/cs/cs425/MJWordCount.class src/main/java/edu/illinois/cs/cs425/MJWordCountThread.class
echo "Cleaning job files"
rm job*
echo "Compiling" 
mvn clean install

