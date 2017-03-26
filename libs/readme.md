
mvn install:install-file -Dfile=rocksdbjni-5.0.0-osx.jar -DgroupId=rocksdb -DartifactId=rocksdb -Dversion=5.0.0 -Dpackaging=jar

mvn install:install-file -Dfile=rocksdbjni-5.0.0-linux64.jar -DgroupId=rocksdb -DartifactId=rocksdb -Dversion=5.0.0 -Dpackaging=jar

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_102.jdk/Contents/Home/

make shared_lib;make rocksdbjava
make rocksdbjavastaticrelease
