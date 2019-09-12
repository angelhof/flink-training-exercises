all:
	mvn -T 6 clean package

no-tests:
	mvn -T 6 clean package -DskipTests
