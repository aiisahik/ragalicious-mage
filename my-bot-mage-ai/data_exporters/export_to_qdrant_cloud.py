if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from langchain_openai.embeddings import OpenAIEmbeddings
from langchain_qdrant.vectorstores import Qdrant
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema.document import Document
import os
import pandas as pd


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
    TARGET_COLLECTION_NAME = kwargs['TARGET_COLLECTION_NAME']

    embeddings_model = OpenAIEmbeddings(
        model="text-embedding-3-small"
    )

    splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        chunk_size = 8000,
        chunk_overlap = 100
    )

    all_docs = []
    for index, doc_row in df.iterrows():
        description = doc_row['page_content']
        row_docs = [
            Document(
                page_content=chunk, 
                metadata=doc_row['metadata']
            ) for chunk in splitter.split_text(description)
        ]
        all_docs += row_docs


    print(len(all_docs))

    qdrant = Qdrant.from_documents(
        all_docs,
        embeddings_model,
        url=os.environ['QDRANT_CLOUD_URL'],
        prefer_grpc=True,
        api_key=os.environ['QDRANT_CLOUD_API_KEY'],
        collection_name=TARGET_COLLECTION_NAME,
        force_recreate=False
    )

    upsert_data = [{
        'url': row['url'],
        'status': 'vector_success',
    } for index, row in df.iterrows()]

    return pd.DataFrame(upsert_data)

