FROM mageai/mageai:latest

VOLUME /home/src

EXPOSE 6789

ENTRYPOINT [ "/app/run_app.sh", "mage", "start", "my-bot-mage-ai" ]
