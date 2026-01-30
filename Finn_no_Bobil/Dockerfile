ARG BUILD_FROM
FROM ${BUILD_FROM}

LABEL maintainer="Kristian Kaldager <kaldager.kristian@gmail.com>"
LABEL io.hass.version="0.70"

RUN apk add --no-cache python3 py3-pip

# Opprett et virtuelt miljø og installer avhengigheter
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Installer Python-avhengigheter
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

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
