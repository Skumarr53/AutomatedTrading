#!/bin/bash

OUTPUT=$(which curl)
CURL_STR='curl'
ULAA_STR="ulaa-browser"
SILENT_INSTALL=$1

execute(){
	if [ "$SILENT_INSTALL" = "--silent" ]; then
		"$@" "-y"
	else
		"$@"
	fi
}

if [[ "${OUTPUT}" == *"${CURL_STR}"* ]]; then
	echo "Curl is installed"
else
	execute sudo apt install curl
fi

IS_INSTALLED=$(which ulaa-browser)

if [[ "${IS_INSTALLED}" == *"${ULAA_STR}"* ]]
then
	echo "Ulaa browser is installed"
	echo "Uninstalling ulaa-browser"
	execute sudo apt remove ulaa-browser
fi

sudo rm /etc/apt/sources.list.d/ulaa-browser-release.list

sudo curl -sSL https://ulaa.zoho.com/release/linux/stable/pubkey | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/ulaa-browser-release.gpg > /dev/null

echo "Added the public key"

echo "deb [signed-by=/etc/apt/trusted.gpg.d/ulaa-browser-release.gpg arch=amd64] https://ulaa.zoho.com/release/linux/stable /" | sudo tee /etc/apt/sources.list.d/ulaa-browser-release.list

sudo apt update

execute sudo apt install ulaa-browser
