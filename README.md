# Engenharia-de-dados
“O projeto deverá contemplar a leitura de dados de um ambiente relacional a sua escolha, copiar para um Data Lake, transformar/refinar e disponibilizar os dados em um modelo dimensional ou OBT para consumo através de um dashboard

## Integrantes
[GuilhermeVolpato](https://github.com/GuilhermeVolpato)<br>
[luizotavio-vieira](https://github.com/luizotavio-vieira)<br>
[GabrielCanarin](https://github.com/GabrielCanarin)<br>
[NaumMarcirio](https://github.com/NaumMarcirio)<br>
[GuilhermeMSilveira](https://github.com/GuilhermeMSilveira)<br>
[CharlesClezar](https://github.com/CharlesClezar)<br>
[pedrohahn](https://github.com/pedrohahn)<br>
[KinhoMilak](https://github.com/KinhoMilak)<br>

### Modelo Físico:
Utilizado [https://dbdiagram.io/]<br>
Arquivo fonte: [https://dbdiagram.io/d/6499ee8e02bd1c4a5e18a355]<code>link pro arquivo</code><br>

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
  
