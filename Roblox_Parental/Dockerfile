ARG BUILD_FROM
FROM ${BUILD_FROM}

LABEL maintainer="Kristian Kaldager <kristian@jomedit.no>"
LABEL io.hass.version="1.0.15"

ENV ADDON_VERSION="1.0.15"

RUN apk add --no-cache python3 py3-pip

COPY requirements.txt /
RUN pip install --no-cache-dir --break-system-packages -r /requirements.txt

COPY rootfs /
RUN chmod +x /etc/s6-overlay/s6-rc.d/roblox/run \
             /etc/s6-overlay/s6-rc.d/roblox/finish

COPY roblox_api.py /usr/bin/roblox_api.py
COPY roblox_ai.py /usr/bin/roblox_ai.py
COPY roblox_poller.py /usr/bin/roblox_poller.py
COPY roblox_web.py /usr/bin/roblox_web.py
RUN chmod +x /usr/bin/roblox_poller.py /usr/bin/roblox_web.py

COPY www /usr/bin/www

RUN mkdir -p /data

EXPOSE 8099
