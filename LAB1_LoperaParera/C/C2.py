from db_connection import Neo4jApp

def run():
    app = Neo4jApp("bolt://localhost:7687", "neo4j", "password123")
    
    # Stage 2: Find venues where 90% of papers cover community keywords 
    query = """
    MATCH (com:Community {name: "Database"})-[:DEFINED_BY]->(kw:Keyword)
    WITH com, collect(kw) AS communityKeywords
    MATCH (p:Paper)-[:PRESENTED_AT|PUBLISHED_IN]->()-[:BELONGS_TO]->(v) 
    WHERE v:Conference OR v:Journal
    WITH com, v, communityKeywords, collect(DISTINCT p) AS allPapers
    WITH com, v, size(allPapers) AS total, 
         [paper IN allPapers WHERE EXISTS { (paper)-[:COVERS]->(k) WHERE k IN communityKeywords }] AS communityPapers
    WITH com, v, total, size(communityPapers) AS countComm
    WHERE total > 0 AND (toFloat(countComm) / total) >= 0.9
    MERGE (v)-[:RELATED_TO]->(com)
    RETURN v.name AS Venue, total AS TotalPapers, countComm AS CommunityPapers;
    """
    
    print("\n--- Stage 2: Identifying Related Venues (90% Threshold) ---")
    results = app.run_query(query)
    if not results:
        print("No venues met the 90% threshold with the current synthetic data.")
    else:
        for record in results:
            percentage = round((record['CommunityPapers'] / record['TotalPapers']) * 100, 2)
            print(f"Venue: {record['Venue']:<25} | Community Papers: {record['CommunityPapers']}/{record['TotalPapers']} ({percentage}%)")
    
    app.close()

if __name__ == "__main__":
    run()