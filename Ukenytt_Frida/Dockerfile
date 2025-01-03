# Bruk baseimage for Home Assistant add-ons
ARG BUILD_FROM
FROM $BUILD_FROM

# Oppsett av metadata
LABEL maintainer="Kristian Kaldager kaldager.kristian@gmail.com"

# Installer avhengigheter
RUN apk add --no-cache python3 py3-pip default-jre

# Kopier filer til containeren
COPY run.sh /
RUN chmod a+x /run.sh

# Start kommandoen
CMD [ "/run.sh" ]
