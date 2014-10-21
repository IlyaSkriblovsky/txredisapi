FROM        ubuntu:14.04
MAINTAINER  Gleicon <gleicon@gmail.com>

RUN         apt-get update && \
            apt-get -y upgrade && \
            apt-get -y install -q build-essential && \
            apt-get -y install -q python-dev libffi-dev libssl-dev python-pip

RUN         pip install service_identity pycrypto && \
            pip install twisted && \
	    pip install hiredis

ADD . /txredisapi
CMD ["bash"]

