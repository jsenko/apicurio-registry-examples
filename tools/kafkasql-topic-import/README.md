Build import: 

mvn clean install

Run import: 

java -jar target/apicurio-registry-tools-kafkasql-topic-import-2.4.3.Final-jar-with-dependencies.jar -b localhost:9092 -f /home/jsenko/projects/work/kafkasql-bug/apicurio-registry-bkp.kcatdump

Run h2:

rm database.mv.db; rm database.trace.db; java -cp h2*.jar org.h2.tools.Server -tcp -tcpAllowOthers -ifNotExists -tcpPort 9999

Run Registry:

java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Dquarkus.datasource.jdbc.url="jdbc:h2:tcp://localhost:9999//home/jsenko/projects/work/repos/github.com/Apicurio/apicurio-registry.git/database" -jar storage/kafkasql/target/apicurio-registry-storage-kafkasql-2.0.3-SNAPSHOT-runner.jar | tee 01.log

Intellij URL:

jdbc:h2:tcp://localhost:9999//home/jsenko/projects/work/repos/github.com/Apicurio/apicurio-registry.git/database

Intellij user / password:

sa / sa