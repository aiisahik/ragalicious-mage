"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""

from langchain_openai.embeddings import OpenAIEmbeddings
from langchain_qdrant.vectorstores import Qdrant
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema.document import Document
from utils.supabase import get_client



embeddings_model = OpenAIEmbeddings(
    model="text-embedding-3-small"
)

splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    chunk_size = 8000,
    chunk_overlap = 100
)

supabase_client = get_client()
response = (
    supabase_client
    .table("recipes")
    .select("id, url, metadata, md_description")
    .eq("status", "parsed")
    .limit(10)
    .execute()
)

all_docs = []
for recipe in response.data:

    title = recipe['metadata']['title']
    description = recipe.get('md_description')

    description = f"{title}\n\n{description}"
    recipe_docs = [
        Document(
            page_content=x, 
            metadata={
                'title': title, 
                'url': recipe['url'],
                'id': recipe['id'],
            }
        ) for x in splitter.split_text(description)
    ]
    all_docs += recipe_docs


print(len(all_docs))
    

# all_docs[5].page_content
# qdrant = Qdrant.from_documents(
#     all_docs,
#     base_embeddings_model,
#     location=":memory:",  # Local mode with in-memory storage only
#     collection_name="my_documents",
# )

qdrant = Qdrant.from_documents(
    all_docs,
    embeddings_model,
    url='https://30591e3d-7092-41c4-95e1-4d3c7ef6e894.us-east4-0.gcp.cloud.qdrant.io',
    prefer_grpc=True,
    api_key='Lx5y_umJpfbTHwzuxd29aZ4n5J_GKFr3gMhoBhOJ2PXWXraEfsOBPg',
    collection_name="recipe_descriptions",
    force_recreate=True
)


