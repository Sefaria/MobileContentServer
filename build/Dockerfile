FROM us-east1-docker.pkg.dev/production-deployment/containers/base-web:3.9-bullseye

ARG sefariaProjectBranch=master

# Django Environment Variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /Sefaria-Project
ENV DJANGO_SETTINGS_MODULE sefaria.settings

# Clone Sefaria-Project
RUN git clone https://github.com/Sefaria/Sefaria-Project --branch ${sefariaProjectBranch} --single-branch

# requirements.txt from Sefaria-Project
# COPY ./Sefaria-Project/requirements.txt /Sefaria-Project/requirements.txt
RUN pip3 install --no-cache-dir -r /Sefaria-Project/requirements.txt

# requirements.txt from MobileContentServer
COPY ./requirements.txt /MobileContentServer/requirements.txt
RUN pip3 install --no-cache-dir -r /MobileContentServer/requirements.txt

RUN mkdir -p /Sefaria-Project/log

COPY ./ /MobileContentServer
WORKDIR /MobileContentServer

ENV FLASK_APP /MobileContentServer/DownloadServer.py
EXPOSE 80

ENTRYPOINT ["gunicorn", "--log-level", "debug", "--bind", "0.0.0.0:80", "wsgi:app"]
