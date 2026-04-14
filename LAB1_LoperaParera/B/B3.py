from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    query = """
    MATCH (p1:Paper)-[:PUBLISHED_IN]->(v1:Volume)-[:BELONGS_TO]->(j1:Journal)
    WHERE p1.year IN [2023,2024] 
    WITH j1, collect(p1) as papers, count(p1) as total_papers
    UNWIND papers as p
    OPTIONAL MATCH (p)-[:CITED_BY]->(p2:Paper) 
    WHERE p2.year = 2025
    WITH j1, total_papers, count(p2) as total_citations
    RETURN j1.name AS Journal, 
           total_citations AS Citations, 
           total_papers AS Papers, 
           toFloat(total_citations) / total_papers AS ImpactFactor
    ORDER BY ImpactFactor DESC;
    """
    
    print("\n--- Journal Impact Factors (2025) ---")
    results = app.run_query(query)
    
    print(f"{'Journal':<25} | {'IF':<6} | {'Citations':<10} | {'Papers'}")
    print("-" * 60)
    for record in results:
        # Handle cases where Impact Factor might be None to avoid printing errors
        imp_factor = round(record['ImpactFactor'], 3) if record['ImpactFactor'] else 0.0
        print(f"{record['Journal']:<25} | {imp_factor:<6} | {record['Citations']:<10} | {record['Papers']}")

    app.close()

if __name__ == "__main__":
    run()