from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import psycopg2
import zipfile
import sqlite3
import os
import json
from datetime import datetime
from typing import Dict, List

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CLOUD CONFIGURATION ---
# This is the only place your database URL will live now
DATABASE_URL = os.environ.get("DATABASE_URL")

# --- DATABASE HELPER ---
# This function connects to the cloud, not your laptop
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# --- ROOT ROUTE ---
@app.get("/")
async def read_index():
    return FileResponse('index.html')

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        if room_id not in self.active_connections: self.active_connections[room_id] = []
        self.active_connections[room_id].append(websocket)
    def disconnect(self, websocket: WebSocket, room_id: str):
        if room_id in self.active_connections and websocket in self.active_connections[room_id]:
            self.active_connections[room_id].remove(websocket)
    async def broadcast(self, message: str, room_id: str):
        if room_id in self.active_connections:
            for connection in self.active_connections[room_id]:
                await connection.send_text(message)

manager = ConnectionManager()

class NewUser(BaseModel):
    email: str
    password: str
    display_name: str

class NewRoom(BaseModel):
    host_id: int
    subject: str
    max_capacity: int
    is_private: bool

@app.post("/register")
def register_user(user: NewUser):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO Users (Email, PasswordHash, DisplayName) VALUES (%s, %s, %s) RETURNING UserID",
            (user.email, user.password, user.display_name)
        )
        new_user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "user_id": new_user_id, "display_name": user.display_name}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/rooms")
def create_room(room: NewRoom):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO Rooms (HostID, Subject, MaxCapacity, IsPrivate) VALUES (%s, %s, %s, %s) RETURNING RoomID",
            (room.host_id, room.subject, room.max_capacity, room.is_private)
        )
        new_room_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "room_id": new_room_id, "subject": room.subject}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/rooms")
def get_rooms(user_id: int = None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        if user_id is not None:
            cur.execute("SELECT RoomID, Subject, MaxCapacity, HostID, IsPrivate FROM Rooms WHERE IsPrivate = FALSE OR HostID = %s", (user_id,))
        else:
            cur.execute("SELECT RoomID, Subject, MaxCapacity, HostID, IsPrivate FROM Rooms WHERE IsPrivate = FALSE")
        rooms = cur.fetchall()
        cur.close()
        conn.close()
        room_list = [{"room_id": r[0], "subject": r[1], "max_capacity": r[2], "host_id": r[3], "is_private": r[4]} for r in rooms]
        return {"status": "success", "rooms": room_list}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.put("/rooms/{room_id}/lock")
def lock_room(room_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE Rooms SET IsPrivate = TRUE WHERE RoomID = %s", (room_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Room locked!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.put("/rooms/{room_id}/unlock")
def unlock_room(room_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE Rooms SET IsPrivate = FALSE WHERE RoomID = %s", (room_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Room unlocked!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/rooms/{room_id}/messages")
def get_room_messages(room_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT u.DisplayName, m.MessageText, TO_CHAR(m.SentAt, 'HH12:MI AM')
            FROM Messages m
            JOIN Users u ON m.UserID = u.UserID
            WHERE m.RoomID = %s
            ORDER BY m.SentAt ASC
        """, (room_id,))
        messages = cur.fetchall()
        cur.close()
        conn.close()
        history = [{"user": r[0], "text": r[1], "time": r[2]} for r in messages]
        return {"status": "success", "messages": history}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/upload-anki")
async def upload_anki(file: UploadFile = File(...)):
    if not file.filename.endswith('.apkg'): return {"status": "error", "message": "Invalid file"}
    temp_zip = f"temp_{file.filename}"
    cards = []
    try:
        with open(temp_zip, "wb") as f: f.write(await file.read())
        with zipfile.ZipFile(temp_zip, 'r') as z:
            db_name = "collection.anki21" if "collection.anki21" in z.namelist() else "collection.anki2"
            z.extract(db_name, "temp_anki_dir")
        conn = sqlite3.connect(f"temp_anki_dir/{db_name}")
        cur = conn.cursor()
        cur.execute("SELECT flds FROM notes")
        rows = cur.fetchall()
        for row in rows:
            fields = row[0].split('\x1f')
            if len(fields) >= 2: cards.append({"front": fields[0], "back": fields[1]})
        conn.close()
    except Exception as e: return {"status": "error", "message": str(e)}
    finally:
        if os.path.exists(temp_zip): os.remove(temp_zip)
    return {"status": "success", "cards": cards}

@app.websocket("/ws/{room_id}/{user_id}/{user_name}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, user_id: int, user_name: str):
    await manager.connect(websocket, room_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("INSERT INTO Messages (RoomID, UserID, MessageText) VALUES (%s, %s, %s)", (int(room_id), user_id, data))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e: print(e)
            await manager.broadcast(data, room_id)
    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)