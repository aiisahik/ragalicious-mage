"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""
# for doc in all_docs: 
#     print(doc.metadata['title'])

from langchain_qdrant.vectorstores import Qdrant

from_cloud_qdrant = Qdrant.from_existing_collection(
    embedding=embeddings_model,
    collection_name="recipe_descriptions",
    url='https://30591e3d-7092-41c4-95e1-4d3c7ef6e894.us-east4-0.gcp.cloud.qdrant.io',
    api_key='Lx5y_umJpfbTHwzuxd29aZ4n5J_GKFr3gMhoBhOJ2PXWXraEfsOBPg',
)

retriever = from_cloud_qdrant.as_retriever()


# results = retriever.invoke("Chicken Salad Recipies")
results = retriever.invoke('Cheesy recipe', k=4)
print(len(results))
print(results[0].page_content)
