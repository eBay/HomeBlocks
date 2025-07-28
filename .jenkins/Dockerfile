# ##########   #######   ############
FROM hub.tess.io/sds/sds_develop:7.x-latest
LABEL description="Automated HomeBlocks compilation"

WORKDIR /output

RUN set -eux; \
    sed -i 's/master/latest/g' /etc/apt/sources.list; \
    apt-get update; \
    apt-get upgrade -y; \
    apt-get install iputils-ping; \
    apt-get install net-tools; \
    rm -rf /var/lib/apt/lists/*;


# executables
COPY test_volume_io /usr/local/bin/test_volume_io
# scripts
COPY io_test.py /usr/local/bin/scripts/io_test.py
COPY long_running.py /usr/local/bin/scripts/long_running.py


EXPOSE 5000
# ##########   #######   ############
