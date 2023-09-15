import chromadb, re, fitz

def split_into_paragraphs(text):
    paragraphs = re.split(r'\n\s*\n', text)
    return paragraphs

doc = fitz.open('D:/Documents/EP 2537_01 (Sistemas SCADA).pdf')
text_content = []

for page_num in range(len(doc)):
    page = doc[page_num]
    text_content.append(page.get_text())

complete_text = "\n".join(text_content)

paragraphs = split_into_paragraphs(complete_text)

chroma_client = chromadb.Client()

collection = chroma_client.create_collection(name="my_collection")

collection.add(
    documents=["This is a document", "This is another document"],
    metadatas=[{"source": "my_source"}, {"source": "my_source"}],
    ids=["1", "2"]
)

results = collection.query(
    query_texts=["This is a query document"],
    n_results=1
)

print(results)