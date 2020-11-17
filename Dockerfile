FROM ubuntu:focal

ARG go_repo="deb http://ppa.launchpad.net/longsleep/golang-backports/ubuntu bionic main"
ARG pg_repo="deb http://apt.postgresql.org/pub/repos/apt/ focal-pgdg main"
ARG ch_repo="deb http://repo.yandex.ru/clickhouse/deb/stable/ main/"
ENV CH_VERS=20.3.19.4
ENV PG_VERS=13
ENV GO_VERS=1.15

## for apt to be noninteractive
ARG DEBIAN_FRONTEND=noninteractive
ARG DEBCONF_NONINTERACTIVE_SEEN=true

RUN apt-get update && apt-get install -y --no-install-recommends apt-transport-https \
        dirmngr \
        gnupg \
        curl \
        ca-certificates \
    && mkdir -p /etc/apt/sources.list.d \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv 56A3D45E \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 \
    && echo $ch_repo > /etc/apt/sources.list.d/clickhouse.list \
    && echo $go_repo > /etc/apt/sources.list.d/golang.list \
    && echo $pg_repo > /etc/apt/sources.list.d/pgdb.list \
    && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install -y --no-install-recommends golang-${GO_VERS} \
            postgresql-${PG_VERS} postgresql-server-dev-${PG_VERS} libpq-dev wget build-essential \
            clickhouse-common-static=${CH_VERS} \
            clickhouse-client=${CH_VERS} \
            clickhouse-server=${CH_VERS} \
            libgcc-7-dev \
            locales \
            tzdata \
            git \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

RUN ln -s /usr/lib/go-${GO_VERS}/bin/go /usr/bin/go
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

WORKDIR /tmp/ext
COPY  . /tmp/ext
## just to make sure `make installcheck` works in /tmp/ext
RUN chown -R postgres:postgres /tmp/ext