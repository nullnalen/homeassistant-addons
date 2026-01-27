ARG BUILD_FROM
FROM ${BUILD_FROM}

# Sett shell til bash for bedre feilhåndtering
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Installer avhengigheter inkludert build-tools for JPype1
RUN apk add --no-cache \
    python3 \
    py3-pip \
    python3-dev \
    openjdk17-jre-headless \
    openjdk17-jdk \
    g++ \
    make

# Sett Java-miljøvariabler for tabula-py
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk" \
    LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk/lib/server"

# Opprett symlink for libjvm.so
RUN ln -sf /usr/lib/jvm/java-17-openjdk/lib/server/libjvm.so /usr/lib/libjvm.so || true

# Installer Python-avhengigheter (med --break-system-packages for Alpine 3.19+)
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Kopier rootfs (S6-overlay konfigurasjon)
COPY rootfs /

# Kopier applikasjon
COPY ukenytt.py /usr/bin/ukenytt.py
RUN chmod +x /usr/bin/ukenytt.py

# Opprett data-mappe
RUN mkdir -p /data/ukenytt

# Eksponer port
EXPOSE 8099

# Labels
LABEL \
    io.hass.name="Ukenytt" \
    io.hass.description="Mottar ukenytt-PDF via HTTP og konverterer til Home Assistant sensorer" \
    io.hass.type="addon" \
    io.hass.version="1.0.0"
