FROM geonode/geonode-base:latest-ubuntu-22.04
RUN rm -rf /usr/src/geonode
RUN git clone https://github.com/GeoNode/geonode.git /usr/src/geonode
RUN cd /usr/src/geonode && git fetch --all && git checkout 4.4.x && cd -
RUN mkdir -p /usr/src/importer

RUN cd ..
COPY . /usr/src/importer/
WORKDIR /usr/src/importer

RUN pip install -r /usr/src/geonode/requirements.txt
RUN pip install --upgrade  -e /usr/src/importer/
RUN pip install coverage
