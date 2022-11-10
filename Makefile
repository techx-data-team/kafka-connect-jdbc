build:
	mvn clean package -DskipTests &&\
	cp target/kafka-connect-jdbc-10.6.1-SNAPSHOT.jar /home/ec2-user/workspace/techx-data-real-time-template/kafka-connect-setup/connect-local/.artifact/