FROM confluentinc/cp-kafka:7.7.0
COPY setup_topics.sh /usr/local/bin/setup_topics.sh
ENTRYPOINT ["bash", "/usr/local/bin/setup_topics.sh"]