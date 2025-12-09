import pandas as pd
from neo4j import GraphDatabase
import ast
import os
from dotenv import load_dotenv

CSV_FILE = "spotify_dataset.csv"
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))

MIN_POPULARITY = 65

def import_data():
    print("Reading CSV")
    
    try:
        df = pd.read_csv(CSV_FILE, usecols=['Artist(s)', 'song', 'Popularity'])
        df.rename(columns={'Artist(s)': 'artists', 'song': 'track_name', 'Popularity': 'popularity'}, inplace=True)
    except ValueError as e:
        print(f"Error reading CSV columns: {e}")
        return

    print(f"Total rows: {len(df)}")

    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    
    df = df[df['popularity'] >= MIN_POPULARITY]
    df = df[df['artists'].str.contains(',')]
        
    print(f"After filtering: {len(df)}")

    batch_data = []
    
    for index, row in df.iterrows():
        raw_artists = row['artists']
        song_title = row['track_name']
        
        artist_list = []
        try:
            if raw_artists.startswith('[') and raw_artists.endswith(']'):
                artist_list = ast.literal_eval(raw_artists)
            else:
                artist_list = [x.strip() for x in raw_artists.split(',')]
        except:
            continue
            
        artist_list = [a.replace("'", "").strip() for a in artist_list]

        for i in range(len(artist_list) - 1):
            batch_data.append({
                "a1": artist_list[i],
                "a2": artist_list[i+1],
                "song": song_title
            })

    print(f"Uploading {len(batch_data)} to DB")
    
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    with driver.session() as session:
        print("Deleting old data...")
        session.run("MATCH (n) DETACH DELETE n")

    query = """
    UNWIND $batch AS row
    MERGE (a1:Artist {name: row.a1})
    MERGE (a2:Artist {name: row.a2})
    MERGE (a1)-[:FEAT {song: row.song}]->(a2)
    """

    df.to_csv("processed_data.csv", index=False)

    with driver.session() as session:
        chunk_size = 1000
        for i in range(0, len(batch_data), chunk_size):
            chunk = batch_data[i:i + chunk_size]
            session.run(query, batch=chunk)
            print(f"Processed {i + len(chunk)} / {len(batch_data)}...")

    driver.close()
    print("Finished.")

if __name__ == "__main__":
    import_data()