from pydantic import BaseModel
import sys

def exit_on_error(message:str):
    print(message, file=sys.stderr)
    sys.exit()

class File(BaseModel):
    file_path: str
    file_name: str

class Memory(BaseModel):
    id: str
    created_at: str
    content: str
    