import chromadb, fitz, re

def remove_non_ascii(strings):
    cleaned_strings = []
    for string in strings:
        # Keeping only ASCII characters from 32 to 126
        cleaned_string = re.sub(r'[^\x20-\x7E\u00C0-\u00FF]', '', string)
        if cleaned_string and not cleaned_string.isspace():
            cleaned_strings.append(cleaned_string)
    return cleaned_strings

def split_into_paragraphs(text):
    # Splitting the text into paragraphs at one or more newline characters
    paragraphs = text.split('.\n')
    # Stripping extra whitespace and filtering out empty paragraphs
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
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

doc = fitz.open('D:\\Downloads\\Angeio - Presentación de Proyecto.pdf')
# doc = fitz.open('F:\\Media\\books\\Libro_de_Enoc.pdf')
text_content = []

for page_num in range(len(doc)):
    page = doc[page_num]
    text_content.append(page.get_text())

complete_text = "\n".join(text_content)

text_blocks = split_into_paragraphs(complete_text)
print(len(text_blocks))
text_blocks = remove_non_ascii(text_blocks)
print(len(text_blocks))

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
    query_texts=["¿Cuál es el motor del módulo de razonamiento?"],
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