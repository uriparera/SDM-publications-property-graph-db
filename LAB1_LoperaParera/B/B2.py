from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    query = """
    MATCH (p:Paper)-[:WRITTEN_BY]->(a:Author)
    MATCH (p:Paper)-[:PRESENTED_AT]->(e:Edition)-[:BELONGS_TO]->(c:Conference)
    WITH c, a, COUNT(DISTINCT e) as ndist
    WHERE ndist >= 4
    RETURN c.name AS Conference, a.name AS Author, ndist AS Editions;
    """
    
    print("\n--- Conference Communities (>= 4 Editions) ---")
    results = app.run_query(query)
    
    for record in results:
        print(f"Conference: {record['Conference']:<20} | Author: {record['Author']:<20} | Editions: {record['Editions']}")

    app.close()

if __name__ == "__main__":
    run()