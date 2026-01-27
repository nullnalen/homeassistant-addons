# Ukenytt Add-on for Home Assistant

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]
![Supports armhf Architecture][armhf-shield]
![Supports armv7 Architecture][armv7-shield]
![Supports i386 Architecture][i386-shield]

Denne add-on-en lar deg laste opp ukenytt-PDF-filer (f.eks. fra barnehage eller skole) via HTTP og konverterer dem automatisk til Home Assistant sensorer.

## Funksjoner

- HTTP API for opplasting av PDF-filer
- Støtte for flere barn - hver får sin egen sensor
- Automatisk parsing av ukeplan-tabeller
- Siri Shortcuts-kompatibel
- Persistente data

## Installasjon

1. Legg til dette repository i Home Assistant:
   - Gå til **Innstillinger** → **Add-ons** → **Add-on Store**
   - Klikk på **⋮** (tre prikker) øverst til høyre
   - Velg **Repositories**
   - Legg til: `https://github.com/nullnalen/ukenytt_addon`

2. Finn "Ukenytt" i add-on listen og installer

3. Konfigurer add-on-en med dine barn

4. Start add-on-en

## Konfigurasjon

```yaml
api_key: "din-hemmelige-nøkkel"  # Valgfritt
children:
  - name: "Frida"
  - name: "Emma"
```

## Bruk

Last opp PDF via HTTP:

```bash
curl -X POST "http://homeassistant.local:8099/upload?child=frida" \
  -F "file=@ukenytt.pdf"
```

Se [DOCS.md](DOCS.md) for full dokumentasjon.

## Support

- [GitHub Issues](https://github.com/nullnalen/ukenytt_addon/issues)
- [Dokumentasjon](DOCS.md)
- [Endringslogg](CHANGELOG.md)

[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg
[armhf-shield]: https://img.shields.io/badge/armhf-yes-green.svg
[armv7-shield]: https://img.shields.io/badge/armv7-yes-green.svg
[i386-shield]: https://img.shields.io/badge/i386-yes-green.svg
