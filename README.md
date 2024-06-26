# Título do projeto

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

# Documentação do Sistema Pingado Café

# Introdução ao Projeto Pingado Café

Bem-vindo à documentação do projeto Pingado Café, um sistema de gerenciamento desenvolvido para a matéria de Engenharia de Dados, utilizando como base um banco relacional de um projeto passado, para alimentar uma tabela Data Lake, e apresentar um Dashboard com resultados da Cafeteria.

## Visão Geral

- O ambiente relacional – origem – tem 6 tabelas, 10.000 linhas para cada tabela principal e com distribuição de datas para os últimos 3 anos ( O banco de dados utilizado foi modelado na matéria de Banco de Dados 2, [Modelo Relacional](https://dbdiagram.io/d/6499ee8e02bd1c4a5e18a355)).
- Foi utilizado a biblioteca Faker do Python, para gerar as massas de dados e popular o ambiente relacional.
- A ingestão dos dados foi feita através do Azure DataBricks (cloud).
- O Data Lake foi criado em cima de um object storage (cloud) usando a arquitetura medalhão (camadas Landing, Bronze, Silver e Gold).
- Os dados serão gravados no object storage no formato Delta Lake nas camadas Bronze, Silver e Gold.
  A transformação será feita através do Apache Spark (Python/pyspark).
- As funções de ingestão, transformação e movimentação dos dados entre as camadas são
  orquestradas e agendadas através da ferramenta Azure DataBricks.
- Os dados serão disponibilizados na camada Gold no formato dimensional (OBT).
- Foram utilizadas 4 KPIs e 2 métricas para compor o dashboard no DataBricks.
- O dashboard consome os dados do modelo OBT, direto da camada gold.
- A documentação completa do trabalho está publicada no MkDocs.

## Objetivo do Projeto

O objetivo do projeto Pingado Café é desenvolver um sistema de gerenciamento de dados que utilize um banco de dados relacional existente para alimentar um Data Lake, possibilitando a criação de um Dashboard para a apresentação de resultados e insights sobre a operação da cafeteria. Este projeto visa a integração e transformação de grandes volumes de dados, aplicando uma arquitetura moderna e eficiente para armazenamento e processamento de dados, com o uso de tecnologias de ponta como Azure DataBricks, Delta Lake e Apache Spark. Com isso, busca-se fornecer uma plataforma robusta para análise de dados, permitindo uma visão detalhada e otimizada do desempenho da cafeteria, através de KPIs e métricas específicas, e a disponibilização desses dados em um formato dimensional (OBT) adequado para a construção de dashboards informativos e úteis.

## Desenho de Arquitetura:

![image](./mkdocs/docs/images/DIAGRAMA%20ETL.png)

## Estrutura da Documentação

Esta documentação foi organizada para guiar você por todas as funcionalidades e componentes do Pingado Café. Aqui, você encontrará:

- **[Apresentação](./index.md)**.
- **[Pré-Requisitos e Ferramentas](./prerequisitos.md)**.
- **[Execução](./comoExecutar.md)**.

Esperamos que esta documentação seja útil e que o Pingado Café ajude a transformar a gestão de sua cafeteria, tornando-a mais eficiente e lucrativa.

## Integrantes

- Charles Clezar
- Gabriel Canarin Salazar
- Guilherme Silveira
- Guilherme Volpato
- João Eduardo Milak Farias
- Luiz Otavio Vieira
- Naum Marcirio
- Pedro Hahn


## Licença

Este projeto está sob a licença (sua licença) - veja o arquivo [LICENSE](https://github.com/GuilhermeVolpato/Engenharia-de-dados/License) para mais detalhes.

## Referências

Cite aqui todas as referências utilizadas neste projeto, pode ser outros repositórios, livros, artigos de internet etc.



