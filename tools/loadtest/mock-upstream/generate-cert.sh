#!/bin/sh
set -eu

output_dir=${1:-./certs}
mkdir -p "$output_dir"
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 30 \
  -keyout "$output_dir/server-key.pem" \
  -out "$output_dir/server-cert.pem" \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
chmod 600 "$output_dir/server-key.pem"
echo "Created $output_dir/server-cert.pem and $output_dir/server-key.pem"
