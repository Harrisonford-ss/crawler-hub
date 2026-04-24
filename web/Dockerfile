FROM caddy:2-alpine
COPY index.html /srv/
COPY data /srv/data/
RUN echo ':8080 {' > /etc/caddy/Caddyfile && \
    echo '  root * /srv' >> /etc/caddy/Caddyfile && \
    echo '  file_server' >> /etc/caddy/Caddyfile && \
    echo '  try_files {path} /index.html' >> /etc/caddy/Caddyfile && \
    echo '}' >> /etc/caddy/Caddyfile
EXPOSE 8080
