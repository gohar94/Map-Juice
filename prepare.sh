#echo "Trying to set the Linux port configuration"
#sudo echo "1" >/proc/sys/net/ipv4/tcp_tw_reuse
echo "Compiling MJWordCount Application"
javac src/main/java/edu/illinois/cs/cs425/MJWordCount.java
echo "Copying MJWordCount Executable File and Cleaning Up"
cp src/main/java/edu/illinois/cs/cs425/MJWordCount.class MJWordCount.class
cp src/main/java/edu/illinois/cs/cs425/MJWordCountThread.class MJWordCountThread.class
rm src/main/java/edu/illinois/cs/cs425/MJWordCount.class src/main/java/edu/illinois/cs/cs425/MJWordCountThread.class
javac src/main/java/edu/illinois/cs/cs425/MJWordCountReducer.java
mv src/main/java/edu/illinois/cs/cs425/MJWordCountReducer.class MJWordCountReducer.class
mv src/main/java/edu/illinois/cs/cs425/MJWordCountReducerThread.class MJWordCountReducerThread.class
echo "Cleaning job files"
rm -rf job*
echo "Compiling and making test instances"
rm -rf ../1
rm -rf ../2
rm -rf ../3
mvn clean install
cp -r ../CS425_MP4 ../1
cp -r ../CS425_MP4 ../2
cp -r ../CS425_MP4 ../3

