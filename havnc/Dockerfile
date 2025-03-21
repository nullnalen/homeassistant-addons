FROM alpine:3.19

LABEL description="Home Assistant Dashboard through VNC"

# Miljøvariabler
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ="UTC" \
    DISPLAY=:0 \
    NOVNC_PORT=8080 \
    VNC_TITLE="Home Assistant Dashboard"

# Installer nødvendige pakker
RUN apk add --no-cache \
    bash \
    ca-certificates \
    curl \
    dbus \
    dbus-x11 \
    font-noto \
    font-noto-cjk \
    git \
    jq \
    nss \
    openssl \
    procps \
    py3-requests \
    python3 \
    py3-pip \
    sed \
    supervisor \
    tigervnc \
    tzdata \
    unzip \
    wget \
    websockify \
    alsa-lib \
    openbox \
    xvfb \
    chromium

RUN pip3 install --break-system-packages pyxdg

# Sett riktig tidssone
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Installer nyere NoVNC (valgfritt)
ARG NOVNC_VERSION=v1.4.0
RUN git clone --depth 1 --branch $NOVNC_VERSION https://github.com/novnc/noVNC.git /opt/novnc

# Rydd opp
RUN rm -rf /var/cache/apk/* /tmp/*

# Klargjør dbus
RUN mkdir -p /run/dbus

# Kopier filer
COPY index.html /opt/novnc/index.html
COPY config /config

# Start container
CMD ["supervisord", "-c", "/config/supervisord.conf"]
