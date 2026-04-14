from neo4j import GraphDatabase

class Neo4jUploader:
    def __init__(self, uri, user, password, database = "neo4j"):
        # Update with your actual Neo4j credentials
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def execute_step(self, step_name, queries):
        print(f"\n--- Starting: {step_name} ---")
        with self.driver.session(database=self.database) as session:
            for query in queries:
                try:
                    session.run(query)
                    print(f"Successfully executed: {query[:60]}...")
                except Exception as e:
                    print(f"Error executing query: {e}")
        print(f"Finished: {step_name} ---")

    def run_upload(self):
        # ------ Adding Constraints ------
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paper) REQUIRE p.id IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (k:Keyword) REQUIRE k.word IS UNIQUE;"
            "CREATE CONSTRAINT IF NOT EXISTS FOR (j:Journal) REQUIRE j.name IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Conference) REQUIRE c.name IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Volume) REQUIRE v.id IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Edition) REQUIRE e.id IS UNIQUE;"
        ]
        self.execute_step("Creating Constraints", constraints)

        # ------ Loading nodes from CSV created by FormatCSV.py ------
        nodes = [
            "LOAD CSV WITH HEADERS FROM 'file:///nodes_journal.csv' AS row FIELDTERMINATOR ';' MERGE (:Journal {name: row.`name:ID`});",
            "LOAD CSV WITH HEADERS FROM 'file:///nodes_conference.csv' AS row FIELDTERMINATOR ';' MERGE (:Conference {name: row.`name:ID`});",
            
            """
            LOAD CSV WITH HEADERS FROM 'file:///nodes_volume.csv' AS row FIELDTERMINATOR ';'
            MERGE (v:Volume {id: row.`volume_id:ID`})
            SET v.number = toInteger(row.`number:int`), v.year = toInteger(row.`year:int`);
            """,
            
            """
            LOAD CSV WITH HEADERS FROM 'file:///nodes_edition.csv' AS row FIELDTERMINATOR ';'
            MERGE (:Edition {id: row.`edition_id:ID`, city: row.`city:string`, year: toInteger(row.`year:int`)});
            """,
            
            "LOAD CSV WITH HEADERS FROM 'file:///nodes_author.csv' AS row FIELDTERMINATOR ';' WITH row WHERE row.`name:ID` IS NOT NULL MERGE (a:Author {name: row.`name:ID`});",
            "LOAD CSV WITH HEADERS FROM 'file:///nodes_keyword.csv' AS row FIELDTERMINATOR ';' MERGE (k:Keyword {word: row.`word:ID`});",
            
            """
            LOAD CSV WITH HEADERS FROM 'file:///clean_articles.csv' AS row FIELDTERMINATOR ';'
            WITH row WHERE row.`article:ID` IS NOT NULL
            MERGE (p:Paper {id: row.`article:ID`})
            SET p.title = COALESCE(row.`title:string`, 'Unknown Title'), 
                p.year = toInteger(toFloat(COALESCE(row.`year:int`, '0'))), 
                p.type = 'Article',
                p.abstract = row.`abstract:string`;
            """,
            
            """
            LOAD CSV WITH HEADERS FROM 'file:///clean_inproceedings.csv' AS row FIELDTERMINATOR ';'
            WITH row WHERE row.`inproceedings:ID` IS NOT NULL
            MERGE (p:Paper {id: row.`inproceedings:ID`})
            SET p.title = COALESCE(row.`title:string`, 'Unknown Title'), 
                p.year = toInteger(toFloat(COALESCE(row.`year:int`, '0'))), 
                p.type = 'Inproceedings',
                p.abstract = row.`abstract:string`;
            """
        ]
        self.execute_step("Loading Nodes", nodes)

        # ------ Loading relations from CSV created by FormatCSV.py ------
        relationships = [
            "LOAD CSV WITH HEADERS FROM 'file:///rels_presented_at.csv' AS row FIELDTERMINATOR ';' MATCH (p:Paper {id: row.`:START_ID`}), (e:Edition {id: row.`:END_ID`}) MERGE (p)-[:PRESENTED_AT]->(e);",
            "LOAD CSV WITH HEADERS FROM 'file:///rels_published_in.csv' AS row FIELDTERMINATOR ';' MATCH (p:Paper {id: row.`:START_ID`}), (v:Volume {id: row.`:END_ID`}) MERGE (p)-[:PUBLISHED_IN]->(v);",
            "LOAD CSV WITH HEADERS FROM 'file:///rels_edition_belongs_to.csv' AS row FIELDTERMINATOR ';' MATCH (e:Edition {id: row.`:START_ID`}), (c:Conference {name: row.`:END_ID`}) MERGE (e)-[:BELONGS_TO]->(c);",
            "LOAD CSV WITH HEADERS FROM 'file:///rels_volume_belongs_to.csv' AS row FIELDTERMINATOR ';' MATCH (v:Volume {id: row.`:START_ID`}), (j:Journal {name: row.`:END_ID`}) MERGE (v)-[:BELONGS_TO]->(j);",
            
            """
            LOAD CSV WITH HEADERS FROM 'file:///rels_written_by_TOTAL.csv' AS row FIELDTERMINATOR ';'
            MATCH (a:Author {name: row.`:START_ID`}), (p:Paper {id: row.`:END_ID`})
            MERGE (p)-[:WRITTEN_BY {corresponding: toBoolean(row.`corresponding_author:boolean`)}]->(a);
            """,
            
            "LOAD CSV WITH HEADERS FROM 'file:///rels_cited_by_TOTAL.csv' AS row FIELDTERMINATOR ';' MATCH (p1:Paper {id: row.`:START_ID`}), (p2:Paper {id: row.`:END_ID`}) MERGE (p1)-[:CITED_BY]->(p2);",
            "LOAD CSV WITH HEADERS FROM 'file:///rels_has_reviewed_TOTAL.csv' AS row FIELDTERMINATOR ';' MATCH (a:Author {name: row.`:START_ID`}), (p:Paper {id: row.`:END_ID`}) MERGE (a)-[:HAS_REVIEWED]->(p);",
            "LOAD CSV WITH HEADERS FROM 'file:///rels_covers_TOTAL.csv' AS row FIELDTERMINATOR ';' MATCH (p:Paper {id: row.`:START_ID`}), (k:Keyword {word: row.`:END_ID`}) MERGE (p)-[:COVERS]->(k);"
        ]
        self.execute_step("Loading Relationships", relationships)

if __name__ == "__main__":
    # Ensure your Neo4j database is running and CSVs are in the import folder
    target_db = "neo4j"
    uploader = Neo4jUploader("bolt://localhost:7687", "neo4j", "password123", target_db)
    uploader.run_upload()
    uploader.close()
    print("\nDatabase population entirely complete!")