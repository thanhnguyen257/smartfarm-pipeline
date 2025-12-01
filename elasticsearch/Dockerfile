FROM alpine/curl:8.14.1

COPY setup_indices.sh /usr/local/bin/setup_indices.sh
RUN chmod +x /usr/local/bin/setup_indices.sh

ENTRYPOINT ["bash", "/usr/local/bin/setup_indices.sh"]