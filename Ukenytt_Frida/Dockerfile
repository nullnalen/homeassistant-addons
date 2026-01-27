ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.19
FROM $BUILD_FROM

LABEL maintainer="Kristian Kaldager <kaldager.kristian@gmail.com>"

# Installer Python, pip, OpenJDK og nødvendige bygg-verktøy
RUN apk add --no-cache python3 py3-pip openjdk17 g++ gcc libffi-dev python3-dev musl-dev

# Opprett et virtuelt miljø
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Sett Java-miljøvariabler for tabula-py
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk"
ENV LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk/lib/server"

# Opprett symlink for libjvm.so hvis nødvendig
RUN ln -s /usr/lib/jvm/java-17-openjdk/lib/server/libjvm.so /usr/lib/libjvm.so || true

# Installer Python-avhengigheter
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Kopier applikasjonsfiler
COPY ukenytt.py /
COPY run.sh /

# Gjør run.sh kjørbar
RUN chmod a+x /run.sh

# Opprett data-mappe
RUN mkdir -p /data/ukenytt

# Eksponer port for HTTP API
EXPOSE 8099

# Start add-on
CMD [ "/run.sh" ]
