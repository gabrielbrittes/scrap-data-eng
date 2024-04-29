Every inspiration on https://github.com/airscholar/e2e-data-engineering/tree/main and https://www.kaggle.com/code/tiagotgt/desmatamento-total-e-por-estado

Apache Airflow: Responsável pela orquestração da pipeline de dados. Agendamento de tarefas, fluxo de execução e monitoramento são gerenciados pelo Airflow.

Apache Kafka e Zookeeper: Utilizados para o stream de dados provenientes do PostgreSQL para a engine de processamento. Kafka atua como uma plataforma de mensagens distribuída e o Zookeeper é utilizado para coordenar e gerenciar o Kafka.

Control Center and Schema Registry: Usados para monitoramento do schema e para o stream de dados do Kafka. Control Center oferece recursos de gerenciamento e monitoramento do Kafka, enquanto o Schema Registry é responsável pelo armazenamento e gerenciamento dos schemas utilizados no Kafka.

Apache Spark: Utilizado para o processamento de dados em larga escala. Spark oferece funcionalidades para manipulação, transformação e análise de dados de maneira distribuída e paralela.

Cassandra: Banco de dados NoSQL utilizado para armazenamento dos dados processados. Cassandra é escalável e oferece alta disponibilidade, sendo adequado para cenários que demandam grandes volumes de dados e baixa latência.

PostgreSQL: Banco de dados relacional utilizado para armazenamento temporário dos dados antes do processamento pelo Spark.

Docker: Utilizado para criar ambientes isolados e independentes para cada componente da arquitetura, facilitando o gerenciamento e a distribuição dos serviços.


![Captura de tela de 2024-04-28 21-17-22](https://github.com/gabrielbrittes/scrap-data-eng/assets/31296539/1f8cffaf-6725-432e-b5c1-19639a669408)
![Captura de tela de 2024-04-28 21-17-54](https://github.com/gabrielbrittes/scrap-data-eng/assets/31296539/1a4a6156-3202-466f-a6cb-9b2e5d68b1ca)
![Captura de tela de 2024-04-28 21-17-44](https://github.com/gabrielbrittes/scrap-data-eng/assets/31296539/545a915a-8dc7-491e-81c4-f22c22aedb06)
