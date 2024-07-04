FROM mageai/mageai:latest

RUN mkdir -p /home/src

COPY ./requirements.txt  /home/src/my-bot-mage-ai/requirements.txt

# WORKDIR /home/src/my-bot-mage-ai

# RUN pip install pip-tools
# RUN cat requirements.txt
RUN pip install -r /home/src/my-bot-mage-ai/requirements.txt

EXPOSE 6789

ENTRYPOINT [ "/app/run_app.sh", "mage", "start", "my-bot-mage-ai" ]
