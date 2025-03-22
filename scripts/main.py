import os
import google.generativeai as palm
from neo4j import GraphDatabase
import google.generativeai as genai
import re 
# from dotenv import load_dotenv
# ========== Neo4j Connection ==========
uri = "bolt://localhost:7687"
user = "neo4j"
password = "Neo4j1234"
driver = GraphDatabase.driver(uri, auth=(user, password))

GEMINI_API_KEY = "AIzaSyCsbUbk2VcSdidS1zS3hbLJ23itCxpgtNQ"
genai.configure(api_key=GEMINI_API_KEY) 


model = genai.GenerativeModel("gemini-1.5-flash")

def generate_cypher_query(question: str) -> str:
    """ 让 Gemini 生成 Cypher 查询，并去除 Markdown 代码块 """

    system_prompt = """You are an assistant that writes Cypher queries for a Neo4j database.
We have these labels:
 - Subject(id, activity, situation)
 - Incident(id, type)
 - Location(name)
 - Weather(rain, wind, snow, clear)
Relationships:
 - (Subject)-[:SUBJECT_INVOLVED_IN]->(Incident)
 - (Incident)-[:OCCURRED_AT]->(Location)
Please output a SINGLE Cypher query that helps answer the user's question.
Do NOT include ```cypher or any Markdown formatting.
"""

    user_prompt = f"User question: {question}\nCypher Query:"

    response = model.generate_content(system_prompt + "\n" + user_prompt)

    if response and response.text:
        cypher_query = response.text.strip()

        cypher_query = re.sub(r"```(cypher)?", "", cypher_query).strip()

        if "MATCH" not in cypher_query:
            cypher_query = """
            MATCH (s:Subject)-[:SUBJECT_INVOLVED_IN]->(i:Incident)-[:OCCURRED_AT]->(l:Location)
            RETURN s.id AS subject_id, i.id AS incident_id, l.name AS location LIMIT 5
            """
        
        return cypher_query
    else:
        return """
        MATCH (s:Subject)-[:SUBJECT_INVOLVED_IN]->(i:Incident)-[:OCCURRED_AT]->(l:Location)
        RETURN s.id AS subject_id, i.id AS incident_id, l.name AS location LIMIT 5
        """


def run_cypher(query: str):
    with driver.session() as session:
        result = session.run(query)
        records = []
        for record in result:
            row_dict = {}
            for key in record.keys():
                row_dict[key] = record[key]
            records.append(row_dict)
        return records

def answer_with_llm(results, question: str) -> str:

    results_str = "\n".join(str(r) for r in results)

    system_prompt = "You are a helpful assistant that interprets query results."
    user_prompt = f"""
User question: {question}

We have these query results:
{results_str}

Please provide a concise answer in natural language.
If no relevant result, say so politely.
"""

    response = model.generate_content(system_prompt + "\n" + user_prompt)

    if response and response.text:
        return response.text.strip()
    else:
        return "Sorry, I couldn't process your request."

def main():
    question = "Where are the missing hikers located?"  
    cypher_query = generate_cypher_query(question)
    print("[Generated Cypher] ", cypher_query)

    data = run_cypher(cypher_query)
    print("[Cypher Query Results] ", data)

    final_answer = answer_with_llm(data, question)
    print("[Final Answer] ", final_answer)

if __name__ == "__main__":
    main()
