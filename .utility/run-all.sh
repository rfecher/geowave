#!/bin/bash
(sudo service ntp stop && sudo ntpd -gq && sudo service ntp start) || true

lsb_release -a | grep "xenial" \
&& export PATH=${PATH/:\/usr\/local\/lib\/jvm\/openjdk11\/bin/} \
&& sudo apt-get install openjdk-8-jdk \
&& sudo update-java-alternatives -s java-1.8.0-openjdk-amd64 \
&& export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
|| true
.utility/maven-coveralls-hack.sh
chmod +x .utility/*.sh

.utility/build-dev-resources.sh

.utility/build.sh

.utility/run-tests.sh
.utility/publish.sh