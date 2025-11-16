#!/bin/bash
set -e

if ! command -v docker &> /dev/null
then
    echo "docker n'est pas installé. Veuillez l'installer avant de continuer."
    exit 1
fi

if ! command -v wget &> /dev/null
then
    echo "wget n'est pas installé. Veuillez l'installer avant de continuer."
    exit 1
fi

PENTAHO_VERSION="9.3.0.0-428"
PENTAHO_ZIP="pentaho-server-ce-${PENTAHO_VERSION}.zip"
DOWNLOAD_URL="https://github.com/ambientelivre/legacy-pentaho-ce/releases/download/pentaho-server-ce-${PENTAHO_VERSION}/${PENTAHO_ZIP}"

if [ ! -f "${PENTAHO_ZIP}" ]; then
    echo "Téléchargement de Pentaho Server ${PENTAHO_VERSION}..."
    wget -q --show-progress "${DOWNLOAD_URL}" -O "${PENTAHO_ZIP}"
else
    echo "L'archive Pentaho est dejà présente."
fi

mkdir tmp/
cp -r ../../restitutions/cde tmp/cde
cp -r ../../restitutions/dashboard tmp/dashboard
cd tmp
zip -r cde.zip cde
zip -r dashboard.zip dashboard