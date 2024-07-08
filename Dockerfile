FROM mageai/mageai:latest

RUN mkdir -p /home/src

COPY ./my-bot-mage-ai/ /home/src/my-bot-mage-ai/
COPY ./requirements.txt  /home/src/my-bot-mage-ai/requirements.txt
COPY ./.gitignore /home/src/.gitignore

RUN pip install -r /home/src/my-bot-mage-ai/requirements.txt
RUN pip install -U hrequests[all]
RUN python -m hrequests install
ENV PYTHONPATH "${PYTHONPATH}:/home/src/my-bot-mage-ai"

EXPOSE 6789

ENTRYPOINT [ "/app/run_app.sh", "mage", "start", "my-bot-mage-ai" ]