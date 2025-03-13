FROM alpine:3.19

LABEL AboutImage="Home Assistant Dashboard through VNC"

# Locale
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ="UTC"

RUN apk update && \
    apk add --no-cache tzdata ca-certificates supervisor curl wget openssl bash python3 py3-requests sed unzip xvfb tigervnc websockify openbox chromium nss alsa-lib font-noto font-noto-cjk jq git procps dbus dbus-x11

# TimeZone
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone

# Install NoVNC
ARG NOVNC_VERSION=v0.5.1
RUN git clone -c http.sslVerify=false --branch $NOVNC_VERSION https://github.com/novnc/noVNC.git /opt/novnc

# Cleanup to reduce image size
RUN rm -rf /var/cache/apk/* /tmp/*

RUN mkdir -p /var/run/dbus && rm -f /var/run/dbus/system_bus_socket /run/dbus/dbus.pid

# Copy configuration files
COPY index.html /opt/novnc/index.html
COPY config /config

# Define environment variables
ENV PORT=8080
ENV NOVNC_PORT=8080
ENV VNC_TITLE="Home Assistant Dashboard"
ENV DISPLAY=:0

CMD ["supervisord", "-l", "/var/log/supervisord.log", "-c", "/config/supervisord.conf"]
