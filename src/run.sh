#!/bin/bash

echo "Hello World!"

gnome-terminal -e "bash -c python\ Server/directoryServer.py;bash"

gnome-terminal -e "bash -c python\ Server/fileServer.py;bash"
gnome-terminal -e "bash -c python\ Server/fileServer.py;bash"

gnome-terminal -e "bash -c python\ Server/lockingServer.py;bash"

gnome-terminal -e "bash -c python\ Client/client.py;bash"

python Client/client.py localhost 8080

$SHELL