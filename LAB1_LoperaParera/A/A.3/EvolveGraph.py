from neo4j import GraphDatabase

class GraphEvolver:
    def __init__(self, uri, user, password, database="neo4j"):
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
                    print("Successfully executed query.")
                except Exception as e:
                    print(f"Error executing query: {e}")
        print(f"--- Finished: {step_name} ---")

    def run_evolution(self):
        # ------ Add venue reviewers conditions ------
        venues = [
            "MATCH (j:Journal) SET j.required_reviewers = toInteger(rand() * 3) + 2;",
            "MATCH (c:Conference) SET c.required_reviewers = toInteger(rand() * 3) + 2;"
        ]
        self.execute_step("Setting Venue Requirements", venues)

        # ------ Add organizations synthetically ------
        organizations = [
            """
            MERGE (o1:Organization {name: "Saltoki", type: "Company"})
            MERGE (o2:Organization {name: "UPC", type: "University"})
            MERGE (o3:Organization {name: "TU Delft", type: "University"})
            MERGE (o4:Organization {name: "BSC-CNS", type: "Company"})
            MERGE (o5:Organization {name: "Blanes University", type: "University"})
            MERGE (o6:Organization {name: "Sansa Institute", type: "University"})
            MERGE (o7:Organization {name: "BMW Group", type: "Company"});
            """,
            """
            MATCH (a:Author)
            MATCH (o:Organization)
            WITH a, collect(o) AS orgs
            WITH a, orgs, toInteger(rand() * size(orgs)) AS index
            WITH a, orgs[index] AS selected_org
            MERGE (a)-[:AFFILIATED_WITH]->(selected_org);
            """
        ]
        self.execute_step("Creating and Assigning Organizations", organizations)

        # ------ Add review content to relations ------
        reviews = [
            """
            MATCH ()-[r:HAS_REVIEWED]->()
            SET r.decision = rand() > 0.25, 
                r.content = "This is a synthetically generated review detailing methodology feedback.";
            """
        ]
        self.execute_step("Enriching Review Relations", reviews)

        # ------ Calculate paper acceptance using statement condition ------
        acceptance = [
            """
            MATCH (p:Paper)-[:PUBLISHED_IN|PRESENTED_AT]->(venue_container)-[:BELONGS_TO]->(venue)
            MATCH (p)<-[r:HAS_REVIEWED]-()
            WITH p, venue, 
                 count(r) AS actual_reviews, 
                 sum(CASE WHEN r.decision = true THEN 1 ELSE 0 END) AS positive_reviews
            SET p.accepted = (actual_reviews >= venue.required_reviewers AND 
                              positive_reviews > (actual_reviews / 2.0));
            """
        ]
        self.execute_step("Calculating Paper Acceptance", acceptance)


if __name__ == "__main__":
    TARGET_DB = "neo4j" 
    
    evolver = GraphEvolver("bolt://localhost:7687", "neo4j", "password123", TARGET_DB)
    evolver.run_evolution()
    evolver.close()
    
    print("\nGraph Evolution Complete! Ready for querying.")