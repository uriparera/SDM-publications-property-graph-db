from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    # Stage 4 cleanup 
    cleanup = "MATCH (a:Author) WHERE a:PotentialReviewer OR a:Guru REMOVE a:PotentialReviewer, a:Guru;"
    app.run_query(cleanup)

    
    query = """
    MATCH (p:TopPaper)-[:WRITTEN_BY]->(a:Author)
    WITH a, count(p) AS writtenPapers
    SET a:PotentialReviewer
    WITH a, writtenPapers
    WHERE writtenPapers >= 2 
    SET a:Guru
    RETURN a.name AS Author, writtenPapers AS TopPapers, 
           labels(a) AS Roles ORDER BY TopPapers DESC;
    """
    
    print("\n--- Stage 4: Identifying Gurus and Potential Reviewers ---")
    results = app.run_query(query)
    for record in results:
        roles = [r for r in record['Roles'] if r != 'Author']
        print(f"Author: {record['Author']:<25} | Top Papers: {record['TopPapers']} | Roles: {', '.join(roles)}")
    
    app.close()

if __name__ == "__main__":
    run()