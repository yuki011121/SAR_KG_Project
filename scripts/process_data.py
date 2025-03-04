import pandas as pd
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Read Excel files
file1 = os.path.join(script_dir, "../data/NY_IncidentSubjectDataRequest.xlsx")
file2 = os.path.join(script_dir, "../data/NY_IncidentDataRequest.xlsx")

df_subjects = pd.read_excel(file1)
df_incidents = pd.read_excel(file2)

# Preview the data format
print(df_subjects.head())  # Display the first few rows
print(df_incidents.head())

# Select relevant fields
subject_nodes = df_subjects[['SUBJECT ID NUMBER', 'ACTIVITY', 'SITUATION']]
incident_nodes = df_incidents[['INCIDENT ID NUMBER', 'INCIDENT COUNTY', 'RESPONSE TYPE']]
location_nodes = df_incidents[['INCIDENT COUNTY']]
weather_nodes = df_incidents[['WEATHER IND RAIN', 'WEATHER IND WIND', 'WEATHER IND SNOW', 'WEATHER IND CLEAR']]

# Remove duplicates to ensure each entity is unique
location_nodes = location_nodes.drop_duplicates()
weather_nodes = weather_nodes.drop_duplicates()

# Relationship data
subject_to_incident = df_subjects[['SUBJECT ID NUMBER', 'INCIDENT ID NUMBER']]
incident_to_location = df_incidents[['INCIDENT ID NUMBER', 'INCIDENT COUNTY']]
incident_to_weather = df_incidents[['INCIDENT ID NUMBER', 'WEATHER IND RAIN', 'WEATHER IND WIND', 'WEATHER IND SNOW', 'WEATHER IND CLEAR']]

print("Data preprocessing completed!")

from neo4j import GraphDatabase

# Connect to Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "Neo4j1234"

driver = GraphDatabase.driver(uri, auth=(user, password))

# Create nodes
def create_node(tx, label, properties):
    query = f"CREATE (n:{label} $props)"
    tx.run(query, props=properties)

# Create relationships
def create_relationship(tx, node1, label1, node2, label2, relation):
    query = f"""
    MATCH (a:{label1} {{id: $id1}}), (b:{label2} {{id: $id2}})
    CREATE (a)-[:{relation}]->(b)
    """
    tx.run(query, id1=node1, id2=node2)

with driver.session() as session:
    # Insert incident nodes
    for _, row in incident_nodes.iterrows():
        session.write_transaction(create_node, "Incident", {"id": row['INCIDENT ID NUMBER'], "type": row['RESPONSE TYPE']})
    
    # Insert subject nodes
    for _, row in subject_nodes.iterrows():
        session.write_transaction(create_node, "Subject", {"id": row['SUBJECT ID NUMBER'], "activity": row['ACTIVITY'], "situation": row['SITUATION']})
    
    # Insert location nodes
    for _, row in location_nodes.iterrows():
        session.write_transaction(create_node, "Location", {"name": row['INCIDENT COUNTY']})
    
    # Insert weather nodes
    for _, row in weather_nodes.iterrows():
        session.write_transaction(create_node, "Weather", {"rain": row['WEATHER IND RAIN'], "wind": row['WEATHER IND WIND'], "snow": row['WEATHER IND SNOW'], "clear": row['WEATHER IND CLEAR']})
    
    # Create relationships
    for _, row in subject_to_incident.iterrows():
        session.write_transaction(create_relationship, row['SUBJECT ID NUMBER'], "Subject", row['INCIDENT ID NUMBER'], "Incident", "SUBJECT_INVOLVED_IN")
    
    for _, row in incident_to_location.iterrows():
        session.write_transaction(create_relationship, row['INCIDENT ID NUMBER'], "Incident", row['INCIDENT COUNTY'], "Location", "OCCURRED_AT")

print("Data successfully imported into Neo4j!")
