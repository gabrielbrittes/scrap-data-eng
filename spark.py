import logging
import pandas as pd
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import regexp_replace, col

# Configurar o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Função para conectar ao Cassandra
def connect_to_cassandra():
    cluster = Cluster(['localhost'])
    session = cluster.connect()
    return session

# Função para verificar se o keyspace existe
def keyspace_exists(session, keyspace_name):
    try:
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s", [keyspace_name])
        return bool(rows.one())
    except Exception as e:
        logger.error(f"Erro ao verificar a existência do keyspace: {e}")
        return False

# Função para verificar se a tabela existe
def table_exists(session, keyspace_name, table_name):
    try:
        rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s", [keyspace_name, table_name])
        return bool(rows.one())
    except Exception as e:
        logger.error(f"Erro ao verificar a existência da tabela: {e}")
        return False

# Função para criar o keyspace
def create_keyspace(session):
    try:
        session.execute("CREATE KEYSPACE IF NOT EXISTS key_spark WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        logger.info("Keyspace criado com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao criar o keyspace: {e}")

# Função para criar a tabela
def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS key_spark.desmatamentotable (
                ano TEXT PRIMARY KEY,
                acre TEXT,
                amazonas TEXT,
                amapa TEXT,
                maranhao TEXT,
                mato_grosso TEXT,
                para TEXT,
                rondonia TEXT,
                roraima TEXT,
                tocantins TEXT,
                area_total_desmatamento_ano TEXT
            )
        """) 
        logger.info("Tabela criada com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao criar a tabela: {e}")

def connect_to_spark():
    try:
        spark = SparkSession.builder \
            .appName("desmatamento") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        # Definir manualmente o schema para as colunas
        schema = StructType([
            StructField("ano", StringType(), False),
            StructField("acre", StringType(), False),
            StructField("amazonas", StringType(), False),
            StructField("amapa", StringType(), False),
            StructField("maranhao", StringType(), False),
            StructField("mato_grosso", StringType(), False),
            StructField("para", StringType(), False),
            StructField("rondonia", StringType(), False),
            StructField("roraima", StringType(), False),
            StructField("tocantins", StringType(), False),
            StructField("area_total_desmatamento_ano", StringType(), False)
        ])

        # Tive MUITA dificuldade em fazer o kafka comunicar com o spark para fazer uma messageria em tempo real, 
        # não deixarei aplicado de momento neste projeto, estará trabalhando por cima do que é o CSV separado por virgulas de fato.

        data_df = spark.read.csv('/home/gabriel/Downloads/desmatamento/dags/desmatamento.csv', header=True, sep=",", schema=schema)
        #data_df.printSchema()
        # Limpar os valores das colunas, mantendo apenas dígitos e o caractere "-"
        columns_to_clean = [
            "ano", "acre", "amazonas", "amapa", "maranhao", "mato_grosso",
            "para", "rondonia", "roraima", "tocantins", "area_total_desmatamento_ano"
        ]
        for column in columns_to_clean:
            data_df = data_df.withColumn(column, regexp_replace(data_df[column], "[^0-9-]", ""))

        # Executar 40 linhas do DF (Por padrão são apenas 20 linhas.)
        #data_df.show(n=40)
        return data_df
    except Exception as e:
        print(f"Erro ao conectar ao Spark: {e}")
        return None

# Função para inserir os dados no Cassandra
def insert_into_cassandra(data_df, session):
    try:
        # Pré-processamento dos dados para remover linhas com valores indesejados
        cleaned_data_df = data_df.drop()

        insert_statement = session.prepare("""
            INSERT INTO key_spark.desmatamentotable (
                ano, acre, amazonas, amapa, maranhao, mato_grosso,
                para, rondonia, roraima, tocantins, area_total_desmatamento_ano
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for row in cleaned_data_df.collect():
            #print('NO INSERT',row)  # Imprimir a linha para verificação
            # Executar a inserção no Cassandra
            session.execute(insert_statement, tuple(row))

        logger.info("Dados inseridos no Cassandra com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao inserir dados no Cassandra: {e}")

def data_cassandra():
    # Conectar ao Cassandra
    session = connect_to_cassandra()

    # Nome do keyspace e tabela
    keyspace_name = 'key_spark'
    table_name = 'desmatamentotable'

    # Executar consulta para selecionar dados ordenados por ano
    query = f"SELECT * FROM {keyspace_name}.{table_name}"
    rows = session.execute(query)

    # Exibir o resultado da consulta (caso quiser)
    #print("Resultado da consulta:")
    #for row in rows:
        #print(row)

    # Exibir o nome do keyspace e da tabela
    print(f"Keyspace: {keyspace_name}")
    print(f"Tabela: {table_name}")

    # Lista para armazenar as chaves primárias das linhas indesejadas
    invalid_keys = []

    # Iterar sobre as linhas do resultado da consulta
    for row in rows:
        # Verificar se a linha contém valores 'None', 'null', vazios ou caracteres inválidos
        if any(value is None or value == 'None' or value == '' or not str(value).strip() for value in row):
            print(f"Linha com valores indesejados: {row}")
            # Adicionar a chave primária à lista de chaves inválidas
            invalid_keys.append(row[0])  # Acesso à chave primária pelo índice

    # Confirmar com o usuário antes de deletar as linhas
    # Deletar as linhas indesejadas
    
    if invalid_keys:
        confirm = input("Deseja realmente deletar as linhas indesejadas? (s/n): ")
        if confirm.lower() == 's':
            # Deletar as linhas indesejadas
            deleted_count = 0
            for invalid_key in invalid_keys:
                delete_query = f"DELETE FROM {keyspace_name}.{table_name} WHERE ano = '{invalid_key}'"
                session.execute(delete_query)
                deleted_count += 1
            
            # Log da quantidade de linhas deletadas
            print(f"{deleted_count} rows affected.")
            print("Linhas deletadas com sucesso.")

        else:
            print("Operação de deleção cancelada.")
    else:
        print("Não foram encontradas linhas indesejadas para deletar.")

def convert_columns_to_int(data_df, columns_to_string):
    """
    Converte as colunas especificadas para o tipo inteiro.
    """
    for column_name in columns_to_string:
        # Verifica se a coluna não é a coluna 'ano' (que parece ser do tipo string)
        if column_name != 'ano':
            data_df = data_df.withColumn(column_name, col(column_name).cast('int'))
    
    return data_df

def calculate_total_area_by_year(data_df):
    """
    Calcula a área total desmatada por ano.
    """
    pandas_df = data_df.toPandas()
    pandas_df = pandas_df[:-1]  # Remove a última linha

    total_area_by_year = pandas_df.groupby('ano')['area_total_desmatamento_ano'].sum().sort_values().sort_index()
    return total_area_by_year

def plot_area_by_year(total_area_by_year):
    """
    Plota o gráfico de área total desmatada por ano.
    """
    min_area = total_area_by_year.min()
    max_area = total_area_by_year.max()
    colors = ['lightblue' if min_area < area < max_area else 'green' if area == min_area else 'black' for area in total_area_by_year]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(total_area_by_year.index, total_area_by_year, color=colors)
    plt.xlabel('Ano')
    plt.ylabel('Área Desmatada (em km2)')
    plt.title('Área Total Desmatada por Ano')

    legend_colors = {'Menor Área': 'green', 'Áreas dentro do intervalo': 'lightblue', 'Maior Área': 'black'}
    legend_labels = [plt.Rectangle((0,0),1,1, color=color, label=label) for label, color in legend_colors.items()]
    plt.legend(legend_labels, legend_colors.keys())

    plt.savefig('img/area_desmatamento.png')

def analyze_data(data_df, columns_to_string):
    """
    Analisa os dados, calcula a área total desmatada por ano e plota o gráfico correspondente.
    """
    # Convertendo as colunas para tipo int
    data_df = convert_columns_to_int(data_df, columns_to_string)

    # Calcular a área total desmatada por ano
    total_area_by_year = calculate_total_area_by_year(data_df)

    # Plotar o gráfico
    plot_area_by_year(total_area_by_year)

def plot_variation(data_df):
    # Converter o DataFrame do Spark para um DataFrame do Pandas
    pandas_df = data_df.toPandas()

    # Obter a última linha do DataFrame
    variation_row = pandas_df.tail(1)

    # Extrair apenas as colunas que representam a variação
    variation_values = variation_row.iloc[:, 1:].values.flatten()  # Ignorar a primeira coluna 'ano'

    # Extrair os valores numéricos e os sinais de cada variação
    numeric_values = [float(value.strip('%')) for value in variation_values]
    signs = ['green' if value < 0 else 'black' for value in numeric_values]

    # Plotar o gráfico de barras para a variação
    plt.figure(figsize=(10, 6))
    bars = plt.bar(pandas_df.columns[1:], numeric_values, color=signs)

    # Adicionar rótulos e título ao gráfico
    plt.xlabel('EStado')
    plt.ylabel('Variação (%)')
    plt.title('Variação de Desmatamento por Estado(Var. 2022-2023)')

    # Exibir o gráfico
    plt.savefig('img/variacao.png')

def calculate_total_area_by_state(data_df, columns_to_string):
    for column_name in columns_to_string:
        # Verifica se a coluna não é a coluna 'ano' ou 'area_total_desmatamento_ano'
        if column_name != 'ano' and column_name != 'area_total_desmatamento_ano':
            data_df = data_df.withColumn(column_name, col(column_name).cast('int'))

    """
    Calcula a área total desmatada por estado ao longo dos anos, excluindo a última coluna de variação e a última linha.
    Retorna um dicionário contendo o nome do estado e a soma da área desmatada para cada estado em todos os anos.
    """
    # Selecionar as colunas relevantes, excluindo a última coluna de variação e 'area_total_desmatamento_ano'
    relevant_columns = [col for col in data_df.columns if col != 'Var. 2022-2023' and col != 'area_total_desmatamento_ano']
    area_by_state = data_df.select(*relevant_columns)

    # Remover a última linha do DataFrame, que contém os dados de variação
    area_by_state = area_by_state.limit(area_by_state.count() - 1)

    # Calcular a soma da área desmatada por estado
    total_area_by_state = {}
    for column_name in relevant_columns[1:]:  # Ignorar a primeira coluna 'ano'
        state_area = area_by_state.groupBy().sum(column_name).collect()[0]
        total_area_by_state[column_name] = state_area[f"sum({column_name})"]

    return total_area_by_state

def plot_area_by_state(total_area_by_state):
    
    # Ordenar os estados alfabeticamente
    total_area_by_state = sorted(total_area_by_state.items())

    # Extrair os nomes dos estados e as áreas desmatadas
    states = [state[0] for state in total_area_by_state]
    areas = [state[1] for state in total_area_by_state]

    # Plotar o gráfico de barras
    plt.figure(figsize=(10, 6))
    bars = plt.barh(states, areas, color='lightblue')

    # Adicionar rótulos e título ao gráfico
    plt.xlabel('Área Desmatada (em km^2)')
    plt.ylabel('Estados')
    plt.title('Desmatamento Total por Estado')

    # Exibir o valor de cada barra
    for index, value in enumerate(areas):
        plt.text(value, index, str(value))

    # Exibir o gráfico
    plt.show()
    plt.savefig('img/total_estado.png')

# Função principal
def main():
    try:
        session = connect_to_cassandra()

        if not keyspace_exists(session, 'key_spark'):
            create_keyspace(session)

        if not table_exists(session, 'key_spark', 'desmatamentotable'):
            create_table(session)

        data_df = connect_to_spark()

        insert_into_cassandra(data_df, session)
     
        data_cassandra()

         # Definir colunas a serem convertidas para int na nossa biblioteca de gráficos
        columns_to_string = ['acre', 'amazonas', 'amapa', 'maranhao', 'mato_grosso', 'para', 'rondonia', 'roraima', 'tocantins', 'area_total_desmatamento_ano']

        # Chamar a função para análise de dados
        analyze_data(data_df, columns_to_string)
        plot_variation(data_df)
        # Chamar a função para calcular a área total desmatada por estado
        total_area_by_state = calculate_total_area_by_state(data_df, columns_to_string) 

        # Chamar a função para plotar o gráfico de desmatamento total por estado
        plot_area_by_state(total_area_by_state)

        logger.info("Operações concluídas com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")

# Chamar a função principal
if __name__ == "__main__":
    main()
