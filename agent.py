from langchain.agents.agent_types import AgentType
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
import pandas as pd
import requests
from sqlalchemy import create_engine
import os

def conexao_db():
    # Substitua as credenciais de acordo com o seu banco de dados
    host=os.getenv('DB_HOST')
    database=os.getenv('DB_DATABASE')
    user=os.getenv('DB_USER')
    password=os.getenv('DB_PASSWORD')
    port="5432" # Para o caso de postgres

    # Criar a string de conexão: Exemplo postgres
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    # Criar o engine de conexão
    engine = create_engine(connection_string)

    return engine

def get_dados():
    engine = conexao_db()
    
    # Prepare the query
    query = """ Sua query """    
    
    # Carregar os dados da consulta SQL em um DataFrame do Pandas
    try:
        df = pd.read_sql_query(query, engine)
        return df
    
    except Exception as e:
       return e

def invoke(pergunta: str):
    # Carrega o conjunto de dados
    df = get_dados()

    llm = ChatOpenAI(temperature=0, model="gpt-3.5-turbo-0613", api_key=os.getenv('OPENAI_API_KEY'))
    agente_prompt_prefix = """
    Você se chama Zé e está trabalhando com dataframe pandas no Python.
    O nome do Dataframe é `df`.
    """

    agent = create_pandas_dataframe_agent(
        llm,
        df,
        prefix=agente_prompt_prefix,
        verbose=True,
        agent_type=AgentType.OPENAI_FUNCTIONS,    
    )
    agent.invoke(pergunta)


# Exemplo de uso
pergunta_ivk = "Sua Pergunta"
data = invoke(pergunta_ivk)
