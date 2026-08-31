ARG BUILD_FROM
FROM ${BUILD_FROM}

LABEL maintainer="Kristian Kaldager <kaldager.kristian@gmail.com>"
LABEL io.hass.version="1.0"
ENV ADDON_VERSION="1.0"

RUN apk add --no-cache python3 py3-pip

COPY requirements.txt /
RUN pip install --no-cache-dir --break-system-packages -r /requirements.txt

COPY rootfs /
RUN chmod +x /etc/s6-overlay/s6-rc.d/baat/run /etc/s6-overlay/s6-rc.d/baat/finish

COPY baat_v2.py /usr/bin/baat_v2.py
COPY baat_web.py /usr/bin/baat_web.py
RUN chmod +x /usr/bin/baat_v2.py /usr/bin/baat_web.py

RUN mkdir -p /data

EXPOSE 8102
