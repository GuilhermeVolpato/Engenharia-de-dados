## Modelo Físico Relacinal:

Ferramenta Utilizada: [dbdiagram](https://dbdiagram.io/)
 
Modelo Utilizado: [Arquivo Fonte](https://dbdiagram.io/d/6499ee8e02bd1c4a5e18a355)
## Desenho de Arquitetura:

![image]()



## SQL Server Docker Image

Esta é a imagem Docker do SQL Server configurada com os dados necessários. Siga os passos abaixo para baixar e executar o contêiner.

## Passos para Baixar e Executar a Imagem

```
docker pull bielsalaz07/cafeteria:v1

docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Eng_Dados_Cafeteria" -p 1433:1433 --name novo-sql-server --hostname novo-sql-server -d bielsalaz07/cafeteria:v1
```

## SQL Server

Nome do servidor: localhost,1433
Logon: sa
Senha: Eng_Dados_Cafeteria

Caso ocorra algum erro no login, no modal de login abrir opções(canto inferior direito), marcar "Certificado de servidor confiavel", e rodar os proximos dois comandos

```
dotnet dev-certs https --clean

dotnet dev-certs https --trust

```

## Ferramentas Utilizadas

ADLS Gen 2

Azure Data Factory

Azure Databricks

