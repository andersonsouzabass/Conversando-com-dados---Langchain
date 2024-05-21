from langchain.agents.agent_types import AgentType
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
import pandas as pd
from sqlalchemy import create_engine
import requests
import os


def conexao_db():
    # Substitua as credenciais de acordo com o seu banco de dados
    host=''
    database=''
    user=''
    password=''
    port=""

    # Criar a string de conexão
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    # Criar o engine de conexão
    engine = create_engine(connection_string)

    return engine

def get_dados_sinan():
    engine = conexao_db()
    
    # Prepare the query    
    query = """ """ # Sua query
    
    # Carregar os dados da consulta SQL em um DataFrame do Pandas
    try:
        df = pd.read_sql_query(query, engine)
        return df
    
    except Exception as e:
       return e

def get_datalake():
    url = "https://datalake.recife.pe.gov.br/api/buscar"

    payload = {}
    headers = {
    'Authorization': os.getenv('DATALAKE_SINAN')
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        df = pd.json_normalize(response)
        return df
    else:
        response.status_code

def invoke(pergunta: str):
    # Carrega o conjunto de dados
    df = get_dados_sinan()

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

pergunta_ivk = "faça a distribuição de casos de notificados por bairro a partir de 01/01/2024, exiba o top 20"
# pergunta_ivk = "qual o top 5 de agravos de ds_raca_cor parda no bairro CENTRO com mais notificações partir de 01/01/2024."
# pergunta_ivk = "qual é o top 5 de ds_raca_cor dos top 5 agravos dos 5 bairros com mais notificações partir de 01/01/2023?"

data = invoke(pergunta_ivk)

# data = get_dados()
# print(data)
