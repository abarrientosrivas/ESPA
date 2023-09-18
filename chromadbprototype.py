import chromadb, fitz

def split_into_paragraphs(text):
    paragraphs = text.splitlines(keepends=True)
    return paragraphs

def split_into_blocks(text: str, batch_size: int, overlap_size: int) -> list:
    batches = []

    # num_batches = len(text) // batch_size + (1 if len(text) % batch_size != 0 else 0)
    num_batches = len(text) // (batch_size - overlap_size) + 1
    
    for i in range(num_batches):
        start_index = i * (batch_size - overlap_size)
        end_index = start_index + batch_size
        batch = text[start_index:end_index]
        batches.append(batch)

    return batches

def secuence_id_list(size: int) -> list:
    secuence_list = []
    for num in range(size):
        secuence_list.append(str(num))
    return secuence_list

# doc = fitz.open('D:/Documents/EP 2537_01 (Sistemas SCADA).pdf')
doc = fitz.open('F:\\Media\\books\\Libro_de_Enoc.pdf')
text_content = []

for page_num in range(len(doc)):
    page = doc[page_num]
    text_content.append(page.get_text())

complete_text = "\n".join(text_content)

text_blocks = split_into_blocks(complete_text, 1000, 200)

chroma_client = chromadb.Client()

collection = chroma_client.create_collection(name="my_collection")

print("loading database")

collection.add(
    documents=text_blocks,
    metadatas=[{"source": "my_source"}] * len(text_blocks),
    ids=secuence_id_list(len(text_blocks))
)

print("database loaded")

results = collection.query(
    query_texts=["Quienes eran los vigilantes"],
    n_results=5
)

print("=========================================================")
documents = results["documents"][0]
for value in documents:
    print("=========================================================")
    print(str(value))
    print("=========================================================")
print("=========================================================")

print("unloading database")