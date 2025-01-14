ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.19
FROM $BUILD_FROM

LABEL maintainer="Kristian Kaldager <kaldager.kristian@gmail.com>"

# Installer Python, pip og OpenJDK
RUN apk add --no-cache python3 py3-pip

# Opprett et virtuelt miljø og installer avhengigheter
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Installer Python-avhengigheter
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Kopier skript og kjørefiler
COPY bobil.py /
COPY run.sh /

# Gjør run.sh kjørbar
RUN chmod a+x /run.sh

# Start add-on
CMD [ "/run.sh" ]
