FROM mageai/mageai:latest

RUN mkdir -p /home/src


COPY ./requirements.txt  /home/src/my-bot-mage-ai

EXPOSE 6789

ENTRYPOINT [ "/app/run_app.sh", "mage", "start", "my-bot-mage-ai" ]
