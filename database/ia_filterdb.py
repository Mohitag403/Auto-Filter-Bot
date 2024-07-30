import logging
from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import DATABASE_URL, DATABASE_NAME, COLLECTION_NAME, MAX_BTN

client = AsyncIOMotorClient(DATABASE_URL)
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

@instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    faculty = fields.StrField(allow_none=True)
    batch = fields.StrField(allow_none=True)
    sub = fields.StrField(allow_none=True)
    topic = fields.StrField(allow_none=True)
    date = fields.DateTimeField(allow_none=True)

    class Meta:
        indexes = ('$file_name', )
        collection_name = COLLECTION_NAME

async def save_file(media):
    """Save file in database"""

    # TODO: Find better way to get same file_id for same media to avoid duplicates
    file_id = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(media.file_name))
    c = json.loads(media.caption)
    date = datetime.strptime(media.date, "%d-%m-%Y") if c['date'] else None
    try:
        file = Media(
            file_id=file_id,
            file_name=file_name,
            file_size=media.file_size,
            faculty=c['faculty'],
            batch=c['batch'],
            sub=c['sub'],
            topic=c['topic'],
            date=date
        )
    except ValidationError:
        print(f'Saving Error - {file_name}')
        return 'err'
    else:
        try:
            await file.commit()
        except DuplicateKeyError:      
            print(f'Already Saved - {file_name}')
            return 'dup'
        else:
            print(f'Saved - {file_name}')
            return 'suc'

async def get_files(batch=None, sub=None, topic=None, faculty=None):
    query = {}
    
    if batch:
        query['batch'] = {'$regex': re.compile(batch, re.IGNORECASE)}
    
    if sub:
        query['sub'] = {'$regex': re.compile(sub, re.IGNORECASE)}
    
    if topic:
        query['topic'] = {'$regex': re.compile(topic, re.IGNORECASE)}
    
    if faculty:
        query['faculty'] = {'$regex': re.compile(faculty, re.IGNORECASE)}
    
    try:
        files = await Media.find(query).sort('date', 1).to_list(length=100)
    except PyMongoError as e:
        print(f"Error occurred while fetching files: {e}")
        return []
    
    return files
    
async def delete_files(query):
    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
    
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        regex = query
    filter = {'file_name': regex}
    total = await Media.count_documents(filter)
    files = Media.find(filter)
    return total, files

async def get_file_details(query):
    filter = {'file_id': query}
    cursor = Media.find(filter)
    filedetails = await cursor.to_list(length=1)
    return filedetails

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0

            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    return file_id
