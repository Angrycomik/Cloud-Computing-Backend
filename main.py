from fastapi import FastAPI, HTTPException, status, Query
from neo4j import GraphDatabase
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- MODELS ---
class SearchRequest(BaseModel):
    start_artist: str
    end_artist: str

class ArtistModel(BaseModel):
    name: str

class SongModel(BaseModel):
    artist1: str
    artist2: str
    song_name: str

# --- ROUTES ---
@app.get("/")
def read_root():
    return {"status": "Server is running!"}

@app.get("/artists")
def get_artists(search: str = Query(None, min_length=1)):
    query = "MATCH (a:Artist) RETURN a.name AS name ORDER BY name"
    with driver.session() as session:
            result = session.run(query, search=search)
            return [record["name"] for record in result]

@app.post("/connect")
def find_connection(request: SearchRequest):
    if request.start_artist == request.end_artist:
        return {"found": False, "message": "Please select two different artists."}
    
    query = """
    MATCH p = shortestPath((start:Artist {name: $start})-[:FEAT*]-(end:Artist {name: $end}))
    RETURN [n in nodes(p) | n.name] AS path,
           [r in relationships(p) | r.song] AS songs
    """
    with driver.session() as session:
        result = session.run(query, start=request.start_artist, end=request.end_artist).single()
        
        if not result:
            return {"found": False, "message": "Connection not found"}
        
        return {
            "found": True,
            "path": result["path"],
            "songs": result["songs"]
        }

@app.post("/artists", status_code=status.HTTP_201_CREATED)
def create_artist(artist: ArtistModel):
    clean_name = artist.name.strip()
    
    if(clean_name == ""):
        raise HTTPException(status_code=400, detail="Artist name cannot be empty.")
    
    check_query = """
    MATCH (a:Artist)
    WHERE toLower(a.name) = toLower($name)
    RETURN a
    LIMIT 1
    """
    
    with driver.session() as session:
        result = session.run(check_query, name=clean_name).single()
        
        if result:
            raise HTTPException(
                status_code=409,
                detail=f"Artist '{clean_name}' already exists."
            )
        
        create_query = "CREATE (:Artist {name: $name})"
        session.run(create_query, name=artist.name)
        
        return {"message": f"Artist {clean_name} created."}

@app.post("/songs", status_code=status.HTTP_201_CREATED)
def add_song_connection(song: SongModel):
    if song.artist1 == song.artist2:
        raise HTTPException(status_code=400, detail="Cannot connect an artist to themselves.")
    
    query = """
    MATCH (a1:Artist {name: $name1})
    MATCH (a2:Artist {name: $name2})
    MERGE (a1)-[:FEAT {song: $song}]->(a2)
    RETURN a1
    """
    with driver.session() as session:
        result = session.run(query, name1=song.artist1, name2=song.artist2, song=song.song_name)
        
        if not result.single():
             raise HTTPException(status_code=404, detail="One or both artists not found.")

        return {"message": f"Connected {song.artist1} and {song.artist2}."}

@app.delete("/artists/{name}")
def delete_artist(name: str):
    query = "MATCH (a:Artist {name: $name}) DETACH DELETE a"
    with driver.session() as session:
        result = session.run(query, name=name)
        if result.consume().counters.nodes_deleted == 0:
            raise HTTPException(status_code=404, detail="Artist not found")
        
        return {"message": f"Artist {name} deleted."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)