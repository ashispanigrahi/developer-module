#!groovy

node{
   git 'https://github.com/ashispanigrahi/developer-module.git'
}
stage 'CLEAN_DEVELOPER_MODULE'
node {
   	sh 'chmod +x gradlew'
  sh './gradlew clean --info'
   }

stage 'BUILD_DEVELOPER_MODULE'
node {
 sh 'chmod +x gradlew'
  sh './gradlew build --info'

   }

stage 'TEST_DEVELOPER_MODULE'
node {
   	sh 'chmod +x gradlew'
  sh './gradlew test --info'
   	}
stage 'RUN_JAVA_CODE_COVERAGE'
node {
   	sh 'chmod +x gradlew'
  sh './gradlew jacoco --info'
   	}
  stage 'ANALYZE_CODE'
  node {
   	sh 'chmod +x gradlew'
//  sh './gradlew sonarqube --info'
   	}

stage 'ATRIFACT_DEVELOPER_MODULE'
node {
   sh 'chmod +x gradlew'
  sh './gradlew jar --info'
   	}

stage 'ARCHIVE_ARTIFACTS'
node{
  step([$class: 'ArtifactArchiver', artifacts: '**/build/libs/*.jar', fingerprint: true])
}
