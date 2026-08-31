ARG BUILD_FROM
FROM ${BUILD_FROM}

# Sett shell til bash for bedre feilhåndtering
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Installer avhengigheter
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-flask \
    py3-requests \
    lcms2 \
    fontconfig \
    ttf-dejavu

ENV ADDON_VERSION="1.0.32"

# Installer pdfplumber og waitress via pip
RUN pip3 install --no-cache-dir --break-system-packages pdfplumber waitress

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
    io.hass.version="1.0.32"
