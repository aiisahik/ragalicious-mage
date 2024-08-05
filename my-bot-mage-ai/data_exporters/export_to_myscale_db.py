if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


from langchain_community.vectorstores import MyScale
from langchain_openai.embeddings import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema.document import Document
import os
import pandas as pd
import numpy
from utils.metadata import normalize_metadata

splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    chunk_size = 8000,
    chunk_overlap = 100
)

def get_docs(doc_row, key):
    content = doc_row.get(key)
    if content and len(content) > 5: 
        return [
            Document(
                page_content=chunk, 
                metadata=normalize_metadata(doc_row['metadata'])
            ) for chunk in splitter.split_text(content)
        ]
    return []

os.environ["MYSCALE_HOST"] = 'msc-716ee0dc.us-east-1.aws.myscale.com'
os.environ["MYSCALE_PORT"] = '443'
os.environ["MYSCALE_USERNAME"] = 'jzunit_org_default'
os.environ["MYSCALE_PASSWORD"] = 'passwd_XOAQmfDMJd1rVY'

def add_to_myscale(docs, embeddings_model):
    if isinstance(docs, list) and len(docs) > 0:
        vectorstore = MyScale.from_documents(
            docs,
            embeddings_model,
        )
        return vectorstore
    return None

@data_exporter
def export_data(df, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """

    logger = kwargs['logger']

    embeddings_model = OpenAIEmbeddings(
        model="text-embedding-3-small"
    )

    description_docs = []

    for index, doc_row in df.iterrows():
        description_docs += get_docs(doc_row, 'description')

    logger.info(f'Adding to MyScale {len(description_docs)} recipe descriptions v2')
    add_to_myscale(description_docs, embeddings_model)

    upsert_data = [{
        'url': row['url'],
        'status': 'vector_myscale_success',
    } for index, row in df.iterrows()]

    return pd.DataFrame(upsert_data)


