from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    # Project the Graph
    project = "CALL gds.graph.project('citationGraph', 'Paper', 'CITED_BY');"
    
    # Stream PageRank results 
    algo = """
    CALL gds.pageRank.stream('citationGraph')
    YIELD nodeId, score
    RETURN gds.util.asNode(nodeId).title AS Paper, score
    ORDER BY score DESC
    LIMIT 10;
    """
    
    # Clean up
    drop = "CALL gds.graph.drop('citationGraph');"

    print("\n--- Running PageRank (Top 10 Influential Papers) ---")
    app.run_query(project)
    results = app.run_query(algo)
    for i, record in enumerate(results, 1):
        print(f"{i}. [Score: {round(record['score'], 4)}] {record['Paper']}")
    
    app.run_query(drop)
    app.close()

if __name__ == "__main__":
    run()