# Título do projeto

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

O projeto deverá contemplar a leitura de dados de um ambiente relacional, copiar para um Data Lake, transformar/refinar e disponibilizar os dados em um modelo dimensional ou OBT para consumo através de um dashboard

[GuilhermeVolpato](https://github.com/GuilhermeVolpato)<br>
[luizotavio-vieira](https://github.com/luizotavio-vieira)<br>
[GabrielCanarin](https://github.com/GabrielCanarin)<br>
[NaumMarcirio](https://github.com/NaumMarcirio)<br>
[GuilhermeMSilveira](https://github.com/GuilhermeMSilveira)<br>
[CharlesClezar](https://github.com/CharlesClezar)<br>
[pedrohahn](https://github.com/pedrohahn)<br>
[KinhoMilak](https://github.com/KinhoMilak)<br>

## Começando

Essas instruções permitirão que você obtenha uma cópia do projeto em operação na sua máquina local para fins de desenvolvimento e teste.

Consulte **[Implantação](#-implanta%C3%A7%C3%A3o)** para saber como implantar o projeto.


### Modelo Físico Relacinal:
Utilizado [https://dbdiagram.io/]<br>
Arquivo fonte: [https://dbdiagram.io/d/6499ee8e02bd1c4a5e18a355]<code>link pro arquivo</code><br>

### Desenho de Arquitetura:

## Pré-requisitos

Coloqui uma imagem do seu projeto, como no exemplo abaixo:

![image](https://github.com/GuilhermeVolpato/Engenharia-de-dados/blob/mkdocs/mkdocs/images/DIAGRAMA%20ETL.png)

## Pré-requisitos

### SQL Server Docker Image

Esta é a imagem Docker do SQL Server configurada com os dados necessários. Siga os passos abaixo para baixar e executar o contêiner.<br>

### Passos para Baixar e Executar a Imagem

```sh
docker pull bielsalaz07/cafeteria:v1

docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Eng_Dados_Cafeteria" -p 1433:1433 --name novo-sql-server --hostname novo-sql-server -d bielsalaz07/cafeteria:v1
```

### SQL Server

Nome do servidor: localhost,1433<br>
Logon: sa<br>
Senha: Eng_Dados_Cafeteria<br>


Caso ocorra algum erro no login, no modal de login abrir opções(canto inferior direito), marcar "Certificado de servidor confiavel", e rodar os proximos dois comandos<br>

```sh
dotnet dev-certs https --clean

dotnet dev-certs https --trust
```

### ADLS Gen 2

### Azure Data Factory

### Azure Databricks

## Instalação

Uma série de exemplos passo-a-passo que informam o que você deve executar para ter um ambiente de desenvolvimento em execução.

Diga como essa etapa será:

```
Dar exemplos
```

E repita:

```

Até finalizar
```

Termine com um exemplo de como obter dados do sistema ou como usá-los para uma pequena demonstração.

## Implantação

Adicione notas adicionais sobre como implantar isso em um sistema ativo

## Ferramentas utilizadas

Mencione as ferramentas que você usou para criar seu projeto

* Ferramenta 1 + link - Breve descrição
* Ferramenta 2 + link - Breve descrição
* Ferramenta 3 + link - Breve descrição

## Colaboração

Por favor, leia o [COLABORACAO](https://gist.github.com/usuario/colaboracao.md) para obter detalhes sobre o nosso código de conduta e o processo para nos enviar pedidos de solicitação.

Se desejar publicar suas modificações em um repositório remoto no GitHub, siga estes passos:

1. Crie um novo repositório vazio no GitHub.
2. No terminal, navegue até o diretório raiz do projeto.
3. Execute os seguintes comandos:

```bash
git remote set-url origin https://github.com/seu-usuario/nome-do-novo-repositorio.git
git add .
git commit -m "Adicionar minhas modificações"
git push -u origin master
```

Isso configurará o repositório remoto e enviará suas modificações para lá.

## Versão

Fale sobre a versão e o controle de versões para o projeto. Para as versões disponíveis, observe as [tags neste repositório](https://github.com/suas/tags/do/projeto). 

## Autores

Mencione todos aqueles que ajudaram a levantar o projeto desde o seu início

* **Aluno 1** - *Trabalho Inicial* - [(https://github.com/linkParaPerfil)](https://github.com/linkParaPerfil)
* **Aluno 2** - *Documentação* - [https://github.com/linkParaPerfil](https://github.com/linkParaPerfil)

Você também pode ver a lista de todos os [colaboradores](https://github.com/usuario/projeto/colaboradores) que participaram deste projeto.

## Licença

Este projeto está sob a licença (sua licença) - veja o arquivo [LICENSE](https://github.com/jlsilva01/projeto-ed-satc/blob/main/LICENSE) para detalhes.

## Referências

Cite aqui todas as referências utilizadas neste projeto, pode ser outros repositórios, livros, artigos de internet etc.


