ARG MAIBOT_BASE=sengokucola/maibot:v1.4.3
FROM ${MAIBOT_BASE}
COPY host/ /tmp/maibot-passive-memory/
RUN python /tmp/maibot-passive-memory/install.py
COPY plugin/ /opt/maibot-passive-memory/
