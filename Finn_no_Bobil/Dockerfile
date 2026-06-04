ARG BUILD_FROM
FROM ${BUILD_FROM}

LABEL maintainer="Kristian Kaldager <kaldager.kristian@gmail.com>"
LABEL io.hass.version="1.54"

RUN apk add --no-cache python3 py3-pip

# Installer Python-avhengigheter
COPY requirements.txt /
RUN pip install --no-cache-dir --break-system-packages -r /requirements.txt

# Kopier rootfs (S6-overlay konfigurasjon)
COPY rootfs /
RUN chmod +x /etc/s6-overlay/s6-rc.d/bobil/run /etc/s6-overlay/s6-rc.d/bobil/finish

# Kopier skript
COPY bobil_v2.py /usr/bin/bobil_v2.py
COPY bobil_web.py /usr/bin/bobil_web.py
RUN chmod +x /usr/bin/bobil_v2.py /usr/bin/bobil_web.py

RUN mkdir -p /data

# Eksponer ingress-port
EXPOSE 8100
