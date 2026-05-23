FROM quay.io/astronomer/astro-runtime:3.2-4
USER root
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean
USER astro
ENV JAVA_HOME=/usr/lib/jvm/default-java
