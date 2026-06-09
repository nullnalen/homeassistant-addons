ARG BUILD_FROM
FROM ${BUILD_FROM}

LABEL maintainer="Kristian Kaldager <kaldager.kristian@gmail.com>"
LABEL io.hass.version="1.1"
ENV ADDON_VERSION="1.1"

RUN apk add --no-cache python3 py3-pip

# Installer Python-avhengigheter
COPY requirements.txt /
RUN pip install --no-cache-dir --break-system-packages -r /requirements.txt

# Kopier rootfs (S6-overlay konfigurasjon)
COPY rootfs /
RUN chmod +x /etc/s6-overlay/s6-rc.d/campingvogn/run /etc/s6-overlay/s6-rc.d/campingvogn/finish

# Kopier skript
COPY campingvogn_v2.py /usr/bin/campingvogn_v2.py
COPY campingvogn_web.py /usr/bin/campingvogn_web.py
RUN chmod +x /usr/bin/campingvogn_v2.py /usr/bin/campingvogn_web.py

RUN mkdir -p /data

# Eksponer ingress-port
EXPOSE 8101
