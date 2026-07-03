#!/bin/bash
gnome-terminal -- bash -c "source master.sh; go run ./cmd/master"
gnome-terminal -- bash -c "source envsc1.sh; go run ./cmd/node; exec bash"
gnome-terminal -- bash -c "source envsc2.sh; go run ./cmd/node; exec bash"
gnome-terminal -- bash -c "source envsc3.sh; go run ./cmd/node; exec bash"