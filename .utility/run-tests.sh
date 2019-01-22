#!/bin/bash
set -ev

if [ "$IT_ONLY" == "true" ]; then
  echo -e "Skipping unit tests w/ verify...\n"
  mvn -q verify -am -pl test -Dtest=SkipUnitTests -DfailIfNoTests=false  -Dfindbugs.skip -P $MAVEN_PROFILES
else
  echo -e "Running unit tests only w/ verify...\n"
  mvn -q verify -Dfmt.action=validate  -Dfindbugs.skip -P $MAVEN_PROFILES
fi
