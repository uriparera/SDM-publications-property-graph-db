from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    # Stage 3 cleanup: Remove old labels first just in case it was run before
    cleanup = "MATCH (p) WHERE p:TopPaper OR p.isTop100 IS NOT NULL REMOVE p:TopPaper SET p.isTop100 = null;"
    app.run_query(cleanup)

    # Stage 3: Identify Top 100 Community Papers 
    query = """
    MATCH (v)-[:RELATED_TO]->(c:Community {name: "Database"})
    MATCH (p1:Paper)-[:PRESENTED_AT|PUBLISHED_IN]->()-[:BELONGS_TO]->(v)
    MATCH (p1)-[:CITED_BY]->(p2:Paper)
    WHERE EXISTS { (p2)-[:PRESENTED_AT|PUBLISHED_IN]->()-[:BELONGS_TO]->()-[:RELATED_TO]->(c) }
    WITH p1, count(p2) AS citations 
    ORDER BY citations DESC
    LIMIT 100 
    SET p1:TopPaper, p1.isTop100 = true 
    RETURN p1.title AS Title, citations AS Citations;
    """
    
    print("\n--- Stage 3: Identifying Top 100 Database Papers ---")
    results = app.run_query(query)
    for i, record in enumerate(results, 1):
        print(f"{i}. [{record['Citations']} citations] {record['Title']}")
    
    app.close()

if __name__ == "__main__":
    run()