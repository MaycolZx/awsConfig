#!/bin/bash
# Verificar los 3 DataNodes activos en HDFS:
hdfs dfsadmin -report

# Verificar los NodeManagers en YARN:
yarn node -list

# Datos 
# https://dumps.wikimedia.org/other/mediawiki_content_current/eswiki/2026-08-01/xml/bzip2/
wget https://dumps.wikimedia.org/other/mediawiki_content_current/eswiki/2026-08-01/xml/bzip2/eswiki-2026-08-01-p5168647p9944278.xml.bz2
#wget https://dumps.wikimedia.org/other/mediawiki_content_current/eswiki/2026-08-01/xml/bzip2/eswiki-2026-08-01-p5p5168645.xml.bz2
#wget https://dumps.wikimedia.org/other/mediawiki_content_current/eswiki/2026-08-01/xml/bzip2/eswiki-2026-08-01-p9944283p11564479.xml.bz2

bzip2 -d eswiki-2026-08-01-p5168647p9944278.xml.bz2

# Desde el nodo principal
hdfs dfs -mkdir -p /user/motor_busqueda/wikipedia
hdfs dfs -put ./eswiki-2026-08-01-p5168647p9944278.xml /user/motor_busqueda/wikipedia/
#hdfs dfs -put *.xml.bz2 /user/motor_busqueda/wikipedia/

# Crearemos un proyecto con java
sudo yum install maven -y
mvn archetype:generate -DgroupId=com.motorbusqueda -DartifactId=indice-invertido -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

## Limpiar 
cd indice-invertido
rm pom.xml
rm src/main/java/com/motorbusqueda/App.java
rm src/test/java/com/motorbusqueda/AppTest.java

## Ahora modificamos el pom.xml y agregamos el archivo XmlInputFormat
#vim pom.xml
cp ../files/pom.xml .
#vim src/main/java/com/motorbusqueda/XmlInputFormat.java
cp ../files/XmlInputFormat.java src/main/java/com/motorbusqueda/XmlInputFormat.java

## Ahora compilamos el proyecto

mvn clean package
#vim src/main/java/com/motorbusqueda/IndiceInvertido.java

cp ../files/IndiceInvertido.java src/main/java/com/motorbusqueda/IndiceInvertido.java

mvn clean package

# Ahora lanzamos el trabajo al cluster
hadoop jar target/indice-invertido-1.0-SNAPSHOT.jar com.motorbusqueda.IndiceInvertido /user/motor_busqueda/wikipedia /user/motor_busqueda/resultado_indice
