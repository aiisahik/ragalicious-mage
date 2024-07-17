if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from langchain_openai.embeddings import OpenAIEmbeddings
from langchain_qdrant.vectorstores import Qdrant
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema.document import Document
import os
import pandas as pd

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
                metadata=doc_row['metadata']
            ) for chunk in splitter.split_text(content)
        ]
    return []


def add_to_qdrant(docs, embeddings_model, collection_name):
    if isinstance(docs, list) and len(docs) > 0:
        Qdrant.from_documents(
            docs,
            embeddings_model,
            url=os.environ['QDRANT_CLOUD_URL'],
            prefer_grpc=True,
            api_key=os.environ['QDRANT_CLOUD_API_KEY'],
            collection_name=collection_name,
            force_recreate=False
        )

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
    ingredient_docs = []
    nutrition_docs = []

    for index, doc_row in df.iterrows():
        description_docs += get_docs(doc_row, 'description')
        ingredient_docs += get_docs(doc_row, 'ingredients')
        nutrition_docs += get_docs(doc_row, 'nutrition')

    logger.info(f'Adding to Qdrant {len(description_docs)} recipe descriptions')
    add_to_qdrant(description_docs, embeddings_model, 'recipe_descriptions')
    logger.info(f'Adding to Qdrant {len(ingredient_docs)} recipe ingredients')
    add_to_qdrant(ingredient_docs, embeddings_model, 'recipe_ingredients')
    logger.info(f'Adding to Qdrant {len(nutrition_docs)} recipe nutrition')
    add_to_qdrant(nutrition_docs, embeddings_model, 'recipe_nutrition')

    upsert_data = [{
        'url': row['url'],
        'status': 'vector_success',
    } for index, row in df.iterrows()]

    return pd.DataFrame(upsert_data)

