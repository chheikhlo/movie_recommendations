FROM jupyter/pyspark-notebook

WORKDIR /home/jovyan/work

EXPOSE 8888
EXPOSE 8501

RUN pip install pyspark pymongo streamlit kafka-python

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=ipython3
ENV GRANT_SUDO=yes
ENV NB_GID=100
ENV GEN_CERT=yes

USER root
