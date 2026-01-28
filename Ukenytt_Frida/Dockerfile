ARG BUILD_FROM
FROM ${BUILD_FROM}

# Sett shell til bash for bedre feilhåndtering
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Installer avhengigheter - bruk Alpine-pakker for numpy/pandas (unngår musl segfault)
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-numpy \
    py3-pandas \
    py3-flask \
    py3-requests \
    openjdk17-jre-headless

# Sett Java-miljøvariabler for tabula-py
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk" \
    LD_LIBRARY_PATH="/usr/lib/jvm/java-17-openjdk/lib/server"

# Opprett symlink for libjvm.so
RUN ln -sf /usr/lib/jvm/java-17-openjdk/lib/server/libjvm.so /usr/lib/libjvm.so || true

# Installer kun tabula-py og waitress via pip (resten er Alpine-pakker)
RUN pip3 install --no-cache-dir --break-system-packages tabula-py waitress

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
    io.hass.version="1.0.11"
