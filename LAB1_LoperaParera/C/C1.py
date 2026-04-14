from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    # Stage 1: Define research communities
    query = """
    MERGE (c:Community {name: "Database"}) 
    WITH c
    UNWIND ["data management", "indexing", "data modeling", "big data", "data processing", "data storage", "data querying"] AS db_keywords 
    MATCH (k:Keyword {word: db_keywords})
    MERGE (c)-[:DEFINED_BY]->(k) 
    RETURN c.name AS Community, collect(k.word) AS LinkedKeywords;
    """
    
    print("\n--- Stage 1: Defining Research Communities ---")
    results = app.run_query(query)
    for record in results:
        print(f"Community: {record['Community']}")
        print(f"Keywords: {', '.join(record['LinkedKeywords'])}")
    
    app.close()

if __name__ == "__main__":
    run()