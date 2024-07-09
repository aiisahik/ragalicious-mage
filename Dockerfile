FROM mageai/mageai:latest

RUN mkdir -p /home/src

COPY ./my-bot-mage-ai/ /home/src/my-bot-mage-ai/
COPY ./requirements.txt  /home/src/my-bot-mage-ai/requirements.txt

RUN pip install -r /home/src/my-bot-mage-ai/requirements.txt
RUN pip install -U hrequests[all]
RUN python -m hrequests install
ENV PYTHONPATH "${PYTHONPATH}:/home/src/my-bot-mage-ai"

COPY ./.gitignore /home/src/.gitignore
COPY ./requirements.in /home/src/requirements.in
COPY ./railway.toml  /home/src/railway.toml


EXPOSE 6789

ENTRYPOINT [ "/app/run_app.sh", "mage", "start", "my-bot-mage-ai" ]