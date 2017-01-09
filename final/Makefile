main:
	wget http://140.116.179.52/course/cloud/MOCK_DATA.csv
	-hadoop fs -mkdir /final
	-hadoop fs -put -f MOCK_DATA.csv /final
	mv Makefile.tmp Makefile
	-cp -pv /usr/local/hbase/lib/* /usr/local/hadoop/share/hadoop/common 
	-rm /usr/local/hadoop/share/hadoop/common/hadoop-*-2.5.1.jar
	make
	make run
	@echo "*********************************************"
	@echo "*********************************************"
	@echo "   Hello, please enter the following command "
	@echo "   [ hbase shell ]                           "
	@echo "   in shell enter the command [scan 'final'] "
	@echo "*********************************************"
	@echo "*********************************************"

