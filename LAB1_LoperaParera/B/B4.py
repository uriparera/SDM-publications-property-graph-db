from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    query = """
    MATCH (a:Author)<-[:WRITTEN_BY]-(p:Paper)
    OPTIONAL MATCH (p)-[:CITED_BY]->(citing:Paper)
    WITH a, p, count(citing) AS citations
    ORDER BY a.name, citations DESC 
    WITH a, collect(citations) AS citationList
    UNWIND range(0, size(citationList)-1) AS i
    WITH a, citationList, i
    WHERE citationList[i] >= (i + 1)
    RETURN a.name AS Author, max(i + 1) AS HIndex
    ORDER BY HIndex DESC LIMIT 20;
    """
    
    print("\n--- Top 20 Author H-Indexes ---")
    results = app.run_query(query)
    
    print(f"{'Author Name':<25} | {'H-Index'}")
    print("-" * 36)
    for record in results:
        print(f"{record['Author']:<25} | {record['HIndex']}")

    app.close()

if __name__ == "__main__":
    run()