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
    port="5445"

    # Criar a string de conexão
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    # Criar o engine de conexão
    engine = create_engine(connection_string)

    return engine

def get_dados():
    engine = conexao_db()
    
    # Prepare the query
    query = os.environ["DB_QUERY"]
    query_old = """
        select -- Caracterização da Notificação 
            n.nu_notificacao as identificador,
            TO_CHAR(n.dt_notificacao::timestamp, 'YYYYMMDD') AS data_notificacao,   
            cid10.co_categoria_subcategoria as co_cid,   
            cid10.no_categoria_subcategoria as nome_agravo_notificado,   
            TO_CHAR(n.dt_diagnostico_sintoma::timestamp, 'YYYYMMDD') AS dt_primeiros_sintomas,   
            estab.co_cnes as cnes_notificadora_interna,
            dbsinan.decriptografanova(n.no_nome_paciente) as nome, 
            dbsinan.decriptografanova(n.no_nome_mae) as nome_mae,   
            TO_CHAR(n.dt_nascimento::timestamp, 'YYYYMMDD') AS dt_nascimento, 
            n.tp_sexo as sexo,   
            n.tp_raca_cor as raca_cor,  
            CASE 
                WHEN n.tp_raca_cor = '1' THEN 'branca'     
                WHEN n.tp_raca_cor = '2' THEN 'preta'     
                WHEN n.tp_raca_cor = '3' THEN 'amarela'     
                WHEN n.tp_raca_cor = '4' THEN 'parda'     
                WHEN n.tp_raca_cor = '5' THEN 'indígena'     
                WHEN n.tp_raca_cor = '9' THEN 'ignorado'  
            END AS ds_raca_cor,    
            --socio/demográfico n.tp_escolaridade as co_escolaridade,    
            CASE 
                WHEN n.tp_escolaridade = '01' THEN '1ª a 4ª série incompleta do EF'     
                WHEN n.tp_escolaridade = '02' THEN '4ª série completa do EF (antigo 1° grau)'     
                WHEN n.tp_escolaridade = '03' THEN '5ª à 8ª série incompleta do EF (antigo ginásio ou 1° grau)'     
                WHEN n.tp_escolaridade = '04' THEN 'Ensino fundamental completo (antigo ginásio ou 1° grau)'     
                WHEN n.tp_escolaridade = '05' THEN 'Ensino médio incompleto (antigo colegial ou 2° grau)'     
                WHEN n.tp_escolaridade = '06' THEN 'Ensino médio completo (antigo colegial ou 2° grau)'     
                WHEN n.tp_escolaridade = '07' THEN 'Educação superior incompleta'     
                WHEN n.tp_escolaridade = '08' THEN 'Educação superior completa'     
                WHEN n.tp_escolaridade = '09' THEN 'Ignorado'     
                WHEN n.tp_escolaridade = '10' THEN 'Não se aplica'     
                WHEN n.tp_escolaridade = '43' THEN 'Analfabeto'  
            END AS ds_escolaridade,      
            -- Informações clínicas n.tp_gestante as co_idade_gestacional,  
            CASE 
                WHEN n.tp_gestante = '1' THEN '1º Trimestre'     
                WHEN n.tp_gestante = '2' THEN '2º Trimestre'     
                WHEN n.tp_gestante = '3' THEN '3º Trimestre'     
                WHEN n.tp_gestante = '4' THEN 'Idade gestacional ignorada'     
                WHEN n.tp_gestante = '5' THEN 'Não'     
                WHEN n.tp_gestante = '6' THEN 'Não se aplica'     
                WHEN n.tp_gestante = '9' THEN 'ignorado'  
            END AS ds_idade_gestacional,    
            -- Endereço de residência   pais_res.no_pais as ds_pais_residencia,    
            n.nu_cep_residencia as cep_residencia,   
            uf_res.sg_uf as uf_residencia, -- ISO-3166    
            muni_res.co_municipio_ibge as codigo_ibge_municipio_residencia,   
            muni_res.no_municipio as municipio_residencia,   
            dist_res.no_distrito as distrito_residencia,   
            n.no_bairro_residencia as bairro_residencia, 
            n.no_logradouro_residencia as logradouro_residencia, 
            n.nu_residencia as numero_residencia, 
            dbsinan.decriptografanova(n.ds_complemento_residencia) as complemento_residencia, 
            dbsinan.decriptografanova(n.ds_referencia_residencia) as ponto_referencia_residencia, 
            n.tp_zona_residencia as tp_zona_residencia,  
            CASE 
                WHEN n.tp_zona_residencia = '1' THEN 'urbana'     
                WHEN n.tp_zona_residencia = '2' THEN 'rural'     
                WHEN n.tp_zona_residencia = '3' THEN 'periurbana'     
                WHEN n.tp_zona_residencia = '9' THEN 'ignorado'  
            END AS ds_zona_residencia,    
            --geolocalização geo.co_sistema as latitude, 
            geo_2.co_sistema as longitude, 
            --conclusão      
            TO_CHAR(n.dt_investigacao::timestamp, 'YYYYMMDD') AS dt_investigacao,   
            n.ds_observacao as observacao, 
            -- Classificação de final 
            n.tp_classificacao_final as co_classificacao_final,  
            CASE 
                WHEN n.tp_classificacao_final = '1' THEN 'confirmado'     
                WHEN n.tp_classificacao_final = '2' THEN 'descartado'  
            END AS ds_classificacao_final, 
            n.tp_criterio_confirmacao as co_criterio_notificacao,    
            CASE 
                WHEN n.tp_criterio_confirmacao = '1' THEN 'Laboratorial'     
                WHEN n.tp_criterio_confirmacao = '2' THEN 'Clínico-Epidemiológico'  
            END AS ds_criterio_notificacao,      
            n.tp_evolucao_caso as co_evolucao_caso,  
            CASE 
                WHEN n.tp_evolucao_caso = '1' THEN 'Cura'     
                WHEN n.tp_evolucao_caso = '2' THEN 'Óbito pelo agravo notificado'     
                WHEN n.tp_evolucao_caso = '3' THEN 'Óbito por outras causas'     
                WHEN n.tp_evolucao_caso = '9' THEN 'Ignorado'  
            END AS ds_evolucao_caso,    
            n.st_doenca_trabalho as co_relacionada_trabalho,  
            CASE 
                WHEN n.st_doenca_trabalho = '1' THEN 'Sim'     
                WHEN n.st_doenca_trabalho = '2' THEN 'Não'     
                WHEN n.st_doenca_trabalho = '9' THEN 'Ignorado'  
            END AS co_relacionada_trabalho,      
            TO_CHAR(n.dt_obito::timestamp, 'YYYYMMDD') AS dt_obito,   
            TO_CHAR(n.dt_encerramento::timestamp, 'YYYYMMDD') AS dt_encerramento, 
            n.tp_autoctone_residencia as co_autoctone_residencia,  
            CASE 
                WHEN n.tp_autoctone_residencia = '1' THEN 'Sim'     
                WHEN n.tp_autoctone_residencia = '2' THEN 'Não'     
                WHEN n.tp_autoctone_residencia = '3' THEN 'Indeterminado'  
            END AS ds_autoctone_residencia,    
            --Local Provável da Fonte de Infecção 
            pais.co_seq_pais, pais.no_pais as pais_infeccao, -- Montar esse no n8n - Transformação do dado 
            uf.sg_uf as uf_infeccao, 
            n.co_municipio_infeccao as co_ibge_municipio_infeccao,   
            mn.no_municipio as ds_municipio_infeccao, 
            ds.no_distrito as distrito_infeccao, 
            n.no_bairro_infeccao as bairro_infeccao,   
            TO_CHAR(n.dt_digitacao::timestamp, 'YYYYMMDD') AS dt_digitacao,   
            TO_CHAR(now(), 'YYYYMMDD') AS dt_sincronizacao    
        from 
            dbgeral.tb_cid10 cid10, dbsinan.tb_notificacao n 
            LEFT OUTER JOIN dbsinan.tb_unid_notificadora_externa usne ON CAST(n.co_unidad_notificadora_externa AS NUMERIC) = co_seq_unid_notifica_externa 
            LEFT OUTER JOIN dblocalidade.tb_distrito_svs ds ON n.co_distrito_infeccao = ds.co_distrito 
            LEFT OUTER JOIN dbgeral.tb_pais pais ON n.co_pais_infeccao = pais.co_seq_pais 
            -- Henrique - municipio - Local Provável da Fonte de Infecção 
            LEFT OUTER JOIN dbgeral.tb_municipio mn ON n.co_municipio_infeccao = mn.co_municipio_ibge 
            -- Henrique - UF - Local Provável da Fonte de Infecção 
            LEFT OUTER JOIN dbgeral.tb_uf uf ON n.co_uf_infeccao = uf.co_uf_ibge   
            -- Anderson: Unidade notificadora interna 
            LEFT OUTER JOIN dblocalidade.tb_estabelecimento_saude estab ON n.co_unidade_notificacao = estab.co_estabelecimento
            -- Anderson: País de residência
            LEFT OUTER JOIN dbgeral.tb_pais pais_res ON n.co_pais_residencia = pais_res.co_seq_pais   
            -- Anderson: UF de residência
            LEFT OUTER JOIN dbgeral.tb_uf uf_res ON n.co_uf_residencia = uf_res.co_uf_ibge   
            -- Anderson: Município de residência 
            LEFT OUTER JOIN dbgeral.tb_municipio muni_res ON n.co_municipio_residencia = muni_res.co_municipio_ibge   
            -- Anderson: Distrito de residência
            LEFT OUTER JOIN dblocalidade.tb_distrito_svs dist_res ON n.co_distrito_residencia = dist_res.co_distrito   
            -- Anderson: localidade 1 
            LEFT OUTER JOIN dbsinan.tb_geocampo geo ON n.co_geo_campo_1 = geo.co_localidade   
            -- Anderson: localidade 2 --Notificação individual  -- Classificação Final conforme MI  
            LEFT OUTER JOIN dbsinan.tb_geocampo geo_2 ON n.co_geo_campo_2 = geo_2.co_localidade 
        where 
            -- n.tp_notificacao = '2'  
            n.tp_classificacao_final != '3'  
            and n.tp_classificacao_final != '4'  
            and n.tp_classificacao_final != '5'  
            and n.tp_classificacao_final != '8'  
            and n.tp_classificacao_final != '9'  
            and n.tp_classificacao_final != '10'
            and n.co_cid = cid10.co_categoria_subcategoria
            -- Critério de confirmação conforme MI  
            and n.tp_criterio_confirmacao != '3'  
            and n.tp_criterio_confirmacao != '4'  
            and n.tp_criterio_confirmacao != '5'  
            and n.tp_criterio_confirmacao != '6'  
            and n.tp_criterio_confirmacao != '7'  
            and n.tp_criterio_confirmacao != '8'  
            and n.tp_criterio_confirmacao != '9'  
            and n.tp_criterio_confirmacao != '10'
            -- Evolução do caso conforme MI  
            and n.tp_evolucao_caso != '4'  
            and n.tp_evolucao_caso != '5'  
            and n.tp_evolucao_caso != '6'  
            and n.tp_evolucao_caso != '7'  
            and n.tp_evolucao_caso != '8'  
            and n.tp_evolucao_caso != '10'

            and COALESCE(n.nu_cartao_sus, '') <> ''   
            -- and n.tp_gestante is not null   
            -- and n.ds_observacao is not null
            -- limit 1000
        """
    
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


pergunta_ivk = "faça a distribuição de casos de siflis por bairro notificados a partir de 01/01/2024, exiba o top 20"
# pergunta_ivk = "qual o top 5 de agravos de ds_raca_cor parda no bairro CENTRO com mais notificações partir de 01/01/2024."
# pergunta_ivk = "qual é o top 5 de ds_raca_cor dos top 5 agravos dos 5 bairros com mais notificações partir de 01/01/2023?"

data = invoke(pergunta_ivk)