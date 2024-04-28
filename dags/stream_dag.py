from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import csv
from bs4 import BeautifulSoup
import os
from kafka import KafkaProducer
import time
       
# # Fazendo a requisição GET à página
url = 'http://www.obt.inpe.br/OBT/assuntos/programas/amazonia/prodes'
response = requests.get(url)

def request_data():
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {"class": "plain mceItemTable"})

        if table:
                rows = table.find_all('tr')
                pasta_destino = '/home/gabriel/Downloads/desmatamento/dags' 
                arquivo_csv = os.path.join('desmatamento.csv')

                # Cabeçalhos personalizados
                headers = ['ano', 'acre', 'amazonas', 'amapa', 'maranhao', 'mato_grosso', 'para', 'rondonia', 'roraima', 'tocantins', 'area_total_desmatamento_ano']

                with open(arquivo_csv, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)  # Escreve os cabeçalhos no arquivo CSV

                    for row in rows[1:]: #Feito para pelar a primeira linha (índice 0) que é o cabeçalho HTML
                        csv_row = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])] #Procuro pelas tabelas
                        writer.writerow(csv_row)
                        print('Dados coletados e salvos com sucesso no arquivo:', arquivo_csv)
        else:
                print('Tabela não encontrada.')
    else:
        print('Falha na requisição. Código de status:', response.status_code)

request_data() #Faço a chamada da função para ser criado meu arquivo CSV


def read_csv_and_stream_to_kafka():
    
    arquivo_csv = os.path.join('desmatamento.csv')
    # Inicializa o produtor Kafka
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000) #Caso teste localhost:9092 - broker:29092
        
    # Para o arquivo CSV e envia os dados para o Kafka Broker
   
    with open(arquivo_csv, 'r') as file:
            csv_reader = csv.DictReader(file)
            try:
                for row in csv_reader:
                    ano = row['ano']
                    area_total_desmatamento = row['area_total_desmatamento_ano']

                    # Para cada estado, cria uma mensagem separada
                    for estado, valor in row.items():
                        if estado not in {'ano', 'area_total_desmatamento_ano'}:
                          
                            mensagem = {'ano': ano, 'estado': estado, 'valor_estado': valor, 'area_total_desmatamento_ano': area_total_desmatamento}

                            # Envia a mensagem para o Kafka
                            print(f'Enviando: {mensagem}')
                                                       
                            producer.send('dados', key=estado.encode('utf-8'), value=str(mensagem).encode('utf-8'))
                               
            except Exception as e:
                logging.error(f'An error occurred: {e}')
                            
    producer.flush()
    producer.close()

read_csv_and_stream_to_kafka()

default_args = {
    'owner': 'admin',  # Proprietário da DAG
    'start_date': datetime(2024, 4, 30, 18, 00),  # Data de início da DAG(exemplo)
    'retries': 3,  # Número de tentativas em caso de falha
    'retry_delay': timedelta(minutes=5),  # Intervalo entre tentativas de retry
    'email_on_failure': False,  # Desativar email em caso de falha
    'email_on_retry': False  # Desativar email em caso de retry
}

# Feito a configuração da DAG
with DAG('csv_to_kafka',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    stream_csv_to_kafka = PythonOperator(
        task_id='stream_csv_to_kafka',
        python_callable=read_csv_and_stream_to_kafka
    )

