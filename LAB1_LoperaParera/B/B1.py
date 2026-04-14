from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    query = """
    MATCH (c:Conference)<-[:BELONGS_TO]-(e:Edition)<-[:PRESENTED_AT]-(p1:Paper)
    MATCH (p1)-[:CITED_BY]->(p2:Paper)
    WITH c, p1, count(p2) as citations
    ORDER BY citations DESC 
    WITH c, collect({title: p1.title, count: citations})[0..3] as top3Papers
    UNWIND top3Papers AS paper
    RETURN c.name AS Conference, paper.title AS Title, paper.count AS Citations;
    """
    
    print("\n--- Top 3 Cited Papers per Conference ---")
    results = app.run_query(query)
    
    current_conf = ""
    for record in results:
        # Group printing visually by conference
        if record["Conference"] != current_conf:
            print(f"\nConference: {record['Conference']}")
            current_conf = record["Conference"]
        
        print(f"  - [{record['Citations']} citations] {record['Title']}")

    app.close()

if __name__ == "__main__":
    run()