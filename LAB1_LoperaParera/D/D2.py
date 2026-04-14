
from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    # Project undirected collaboration graph 
    project = """
    CALL gds.graph.project(
      'collaborationGraph',
      ['Author', 'Paper'],
      { WRITTEN_BY: { orientation: 'UNDIRECTED' } }
    );
    """
    
    # Run Louvain and collect researchers 
    algo = """
    CALL gds.louvain.stream('collaborationGraph')
    YIELD nodeId, communityId
    WHERE 'Author' IN labels(gds.util.asNode(nodeId))
    RETURN communityId, collect(gds.util.asNode(nodeId).name) AS Researchers
    ORDER BY size(Researchers) DESC
    LIMIT 5;
    """
    
    # Clean up
    drop = "CALL gds.graph.drop('collaborationGraph');"

    print("\n--- Running Louvain (Top 5 Collaboration Communities) ---")
    app.run_query(project)
    results = app.run_query(algo)
    for record in results:
        members = record['Researchers'][:5] # Show first 5 members
        print(f"Community {record['communityId']}: {len(record['Researchers'])} authors.")
        print(f"  Sample members: {', '.join(members)}...")
    
    app.run_query(drop)
    app.close()

if __name__ == "__main__":
    run()