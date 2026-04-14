import pandas as pd
import random
import os
import warnings

# Path configuration
CURRENT_PATH = os.getcwd()
OUTPUT_DIR = CURRENT_PATH + "/formatted_csv" 
RAW_DATA_PATH = CURRENT_PATH + "/output_dblp"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

if not os.path.exists(RAW_DATA_PATH):
    raise FileNotFoundError(f"Raw data is not available in your current directory.")

# Change these parameters to retrieve more or less publications
n_articles = 10000
n_inproceedings = 10000

def run_preprocessing():
    print("Starting CSV Preprocessing...")

    # ------ Articles ------
    print("Processing Articles, Journals, and Volumes...")

    art_headers_file = RAW_DATA_PATH + '/output_article_header.csv'
    art_data_file = RAW_DATA_PATH + '/output_article.csv'

    with open(art_headers_file, 'r', encoding='utf-8') as f:
        art_headers = f.read().strip().split(';')

    df_art = pd.read_csv(art_data_file, sep=';', names=art_headers, nrows=100000, on_bad_lines='skip', low_memory=False)
    df_art_clean = df_art[~df_art['key:string'].astype(str).str.contains('dblpnote', na=False)]
    df_art_clean = df_art_clean.dropna(subset=['title:string', 'year:int']).head(n_articles)

    # Journals
    journals = df_art_clean[['journal:string']].dropna().drop_duplicates()
    journals.rename(columns={'journal:string': 'name:ID'}, inplace=True)
    journals.to_csv(f'{OUTPUT_DIR}/nodes_journal.csv', index=False, sep=';')

    # Volumes
    volumes = df_art_clean[['journal:string', 'volume:string', 'year:int']].dropna(subset=['journal:string', 'volume:string']).drop_duplicates()
    volumes['journal_str'] = volumes['journal:string'].astype(str).fillna('')
    volumes['volume_str'] = volumes['volume:string'].astype(str).fillna('')
    volumes['volume_id:ID'] = volumes['journal_str'] + "_Vol" + volumes['volume_str']
    volumes.rename(columns={'volume:string': 'number:int'}, inplace=True)
    volumes[['volume_id:ID', 'number:int', 'year:int']].to_csv(f'{OUTPUT_DIR}/nodes_volume.csv', index=False, sep=';')

    # Relations: PUBLISHED_IN & BELONGS_TO (Journals)
    published_in = df_art_clean[['article:ID', 'journal:string', 'volume:string']].dropna(subset=['journal:string', 'volume:string']).copy()
    published_in['volume_id:ID'] = published_in['journal:string'].astype(str) + "_Vol" + published_in['volume:string'].astype(str)
    published_in_rels = published_in[['article:ID', 'volume_id:ID']].rename(columns={'article:ID': ':START_ID', 'volume_id:ID': ':END_ID'})
    published_in_rels[':TYPE'] = 'PUBLISHED_IN'
    published_in_rels.to_csv(f'{OUTPUT_DIR}/rels_published_in.csv', index=False, sep=';')

    belongs_to = volumes[['volume_id:ID', 'journal:string']].rename(columns={'volume_id:ID': ':START_ID', 'journal:string': ':END_ID'})
    belongs_to[':TYPE'] = 'BELONGS_TO'
    belongs_to.to_csv(f'{OUTPUT_DIR}/rels_volume_belongs_to.csv', index=False, sep=';')


    # ------ Inproceedings ------
    print("Processing Inproceedings, Conferences, and Editions...")

    inp_headers_file = RAW_DATA_PATH + '/output_inproceedings_header.csv'
    inp_data_file = RAW_DATA_PATH + '/output_inproceedings.csv'

    with open(inp_headers_file, 'r', encoding='utf-8') as f:
        inp_headers = f.read().strip().split(';')

    df_inp = pd.read_csv(inp_data_file, sep=';', names=inp_headers, nrows=100000, on_bad_lines='skip', low_memory=False)
    df_inp_clean = df_inp[~df_inp['key:string'].astype(str).str.contains('dblpnote', na=False)]
    df_inp_clean = df_inp_clean.dropna(subset=['title:string', 'year:int']).head(n_inproceedings)

    # Conferences
    conferences = df_inp_clean[['booktitle:string']].dropna().drop_duplicates()
    conferences.rename(columns={'booktitle:string': 'name:ID'}, inplace=True)
    conferences['type:string'] = 'Conference'
    conferences.to_csv(f'{OUTPUT_DIR}/nodes_conference.csv', index=False, sep=';')

    # Editions and assign synthetic cities
    city_pool = ["Barcelona", "Berlin", "San Francisco", "Tokyo", "London", "Paris", "New York", 
                 "Rome", "Singapore", "Montreal", "Madrid", "Amsterdam", "Zurich", "Beijing", "Seoul", 
                 "Sydney", "Toronto", "Boston", "Seattle", "Austin", "Dubai", "Mumbai", "Stockholm", 
                 "Copenhagen", "Munich", "Vienna", "Lisbon", "Dublin", "Chicago", "Los Angeles"]
    editions = df_inp_clean[['booktitle:string', 'year:int']].dropna().drop_duplicates()
    editions['edition_id:ID'] = editions['booktitle:string'].astype(str) + "_" + editions['year:int'].astype(str)
    editions['city:string'] = [random.choice(city_pool) for _ in range(len(editions))]    
    editions[['edition_id:ID', 'year:int', 'city:string']].to_csv(f'{OUTPUT_DIR}/nodes_edition.csv', index=False, sep=';')

    # Relations: PRESENTED_AT & BELONGS_TO (Conferences)
    presented_at = df_inp_clean[['inproceedings:ID', 'booktitle:string', 'year:int']].dropna().copy()
    presented_at['edition_id:ID'] = presented_at['booktitle:string'].astype(str) + "_" + presented_at['year:int'].astype(str)
    presented_at_rels = presented_at[['inproceedings:ID', 'edition_id:ID']].rename(columns={'inproceedings:ID': ':START_ID', 'edition_id:ID': ':END_ID'})
    presented_at_rels[':TYPE'] = 'PRESENTED_AT'
    presented_at_rels.to_csv(f'{OUTPUT_DIR}/rels_presented_at.csv', index=False, sep=';')

    belongs_to_conf = editions[['edition_id:ID', 'booktitle:string']].rename(columns={'edition_id:ID': ':START_ID', 'booktitle:string': ':END_ID'})
    belongs_to_conf[':TYPE'] = 'BELONGS_TO'
    belongs_to_conf.to_csv(f'{OUTPUT_DIR}/rels_edition_belongs_to.csv', index=False, sep=';')

    # ------ Authors ------
    print("Generating Authors & Written By Relations...")

    # Articles
    art_authors = df_art_clean.assign(author=df_art_clean['author:string[]'].str.split('|')).explode('author').reset_index(drop=True)
    art_authors['corresponding_author:boolean'] = False
    art_authors.loc[art_authors.groupby('article:ID').head(1).index, 'corresponding_author:boolean'] = True
    rels_wrote_art = art_authors[['author', 'article:ID', 'corresponding_author:boolean']].rename(columns={'author': ':START_ID', 'article:ID': ':END_ID'})

    # Inproceedings
    inp_authors = df_inp_clean.assign(author=df_inp_clean['author:string[]'].str.split('|')).explode('author').reset_index(drop=True)
    inp_authors['corresponding_author:boolean'] = False
    inp_authors.loc[inp_authors.groupby('inproceedings:ID').head(1).index, 'corresponding_author:boolean'] = True
    rels_wrote_inp = inp_authors[['author', 'inproceedings:ID', 'corresponding_author:boolean']].rename(columns={'author': ':START_ID', 'inproceedings:ID': ':END_ID'})

    # Combine and save
    rels_wrote_total = pd.concat([rels_wrote_art, rels_wrote_inp])
    rels_wrote_total[':TYPE'] = 'WRITTEN_BY'
    rels_wrote_total.to_csv(f'{OUTPUT_DIR}/rels_written_by_TOTAL.csv', index=False, sep=';')

    # Nodes: Authors
    unique_authors = rels_wrote_total[':START_ID'].unique()
    pd.DataFrame({'name:ID': unique_authors, ':LABEL': 'Author'}).to_csv(f'{OUTPUT_DIR}/nodes_author.csv', index=False, sep=';')

    # ------ Citations ------
    print("Generating Synthetic Citations...")

    all_paper_ids = df_art_clean['article:ID'].tolist() + df_inp_clean['inproceedings:ID'].tolist()
    synthetic_cites = []

    for p_id in all_paper_ids:
        potential_targets = [p for p in all_paper_ids if p != p_id]
        num_cites = random.randint(1, 40)
        cited_papers = random.sample(potential_targets, num_cites)
        for cited in cited_papers:
            synthetic_cites.append({':START_ID': cited, ':END_ID': p_id, ':TYPE': 'CITED_BY'})

    pd.DataFrame(synthetic_cites).to_csv(f'{OUTPUT_DIR}/rels_cited_by_TOTAL.csv', index=False, sep=';')

    # ------ Keywords ------
    print("Generating Keywords...")
    db_keywords = [
        "data management", "indexing", "data modeling", "big data", "data processing", "data storage", "data querying",
        "query optimization", "nosql", "data mining", "computer vision", "machine learning", "distributed systems"
    ]
    pd.DataFrame({'word:ID': db_keywords, ':LABEL': 'Keyword'}).to_csv(f'{OUTPUT_DIR}/nodes_keyword.csv', index=False, sep=';')

    covers_rels = []
    for p_id in df_art_clean['article:ID'].tolist():
        for word in random.sample(db_keywords, random.randint(2, 5)):
            covers_rels.append({':START_ID': p_id, ':END_ID': word, ':TYPE': 'COVERS'})

    for p_id in df_inp_clean['inproceedings:ID'].tolist():
        for word in random.sample(db_keywords, random.randint(2, 5)):
            covers_rels.append({':START_ID': p_id, ':END_ID': word, ':TYPE': 'COVERS'})

    pd.DataFrame(covers_rels).to_csv(f'{OUTPUT_DIR}/rels_covers_TOTAL.csv', index=False, sep=';')

    # ------ Reviewals ------
    print("Generating Reviews...")
    all_authors_list = unique_authors.tolist()
    review_rels = []

    # For Articles
    for p_id in df_art_clean['article:ID'].tolist():
        paper_authors = set(rels_wrote_art[rels_wrote_art[':END_ID'] == p_id][':START_ID'])
        potential = [a for a in all_authors_list if a not in paper_authors]
        if len(potential) >= 3:
            for reviewer in random.sample(potential, 3):
                review_rels.append({':START_ID': reviewer, ':END_ID': p_id, ':TYPE': 'HAS_REVIEWED'})

    # For Inproceedings
    for p_id in df_inp_clean['inproceedings:ID'].tolist():
        paper_authors = set(rels_wrote_inp[rels_wrote_inp[':END_ID'] == p_id][':START_ID'])
        potential = [a for a in all_authors_list if a not in paper_authors]
        if len(potential) >= 3:
            for reviewer in random.sample(potential, 3):
                review_rels.append({':START_ID': reviewer, ':END_ID': p_id, ':TYPE': 'HAS_REVIEWED'})

    pd.DataFrame(review_rels).to_csv(f'{OUTPUT_DIR}/rels_has_reviewed_TOTAL.csv', index=False, sep=';')

    # ------ Papers ------
    print("Finalizing Paper Nodes...")

    df_art_clean['abstract:string'] = "This paper explores the concepts behind " + df_art_clean['title:string'] + ". We present novel findings and methodologies."
    df_art_clean.to_csv(f'{OUTPUT_DIR}/clean_articles.csv', index=False, sep=';')

    df_inp_clean['abstract:string'] = "This study discusses " + df_inp_clean['title:string'] + ", offering new perspectives for the conference attendees."
    df_inp_clean.to_csv(f'{OUTPUT_DIR}/clean_inproceedings.csv', index=False, sep=';')


    print("All processing finished! Files now can be imported to Neo4j.")


if __name__ == "__main__":
    run_preprocessing()