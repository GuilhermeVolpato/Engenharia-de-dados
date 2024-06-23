## SQL Server Docker Image

Esta é a imagem Docker do SQL Server configurada com os dados necessários. Siga os passos abaixo para baixar e executar o contêiner.

## Passos para Baixar e Executar a Imagem

```
docker pull bielsalaz07/cafeteria:v1
```

```
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Eng_Dados_Cafeteria" -p 1433:1433 --name novo-sql-server --hostname novo-sql-server -d bielsalaz07/cafeteria:v1
```

## SQL Server

Nome do servidor: localhost, 1433 <br>
Logon: sa <br>
Senha: Eng_Dados_Cafeteria <br>

Caso ocorra algum erro no login, no modal de login abrir opções(canto inferior direito), marcar "Certificado de servidor confiavel", e rodar os proximos dois comandos

```
dotnet dev-certs https --clean

dotnet dev-certs https --trust

```

## Criando Azure Data Lake Storage no Azure

#### Disponibilizado por [jlsilva01](https://github.com/jlsilva01)

### Pré-requisitos:

- [Azure CLI](https://learn.microsoft.com/pt-br/cli/azure/)
- [Visual Studio Code](https://code.visualstudio.com/download)
- [Terraform](https://www.terraform.io/downloads)
- Conta Microsoft

### Passos:

1 - Efetuar o login no Azure através do Azure CLI.

```sh
az login
```

2 - Conferir sua assinatura atual.

```sh
az account show -o table
```

3 - Listar todas as assinaturas do Azure da sua conta Microsoft, utilizando o comando abaixo (troque o e-mail abaixo pelo e-mail da sua conta Azure).

```sh
az account list --query "[?user.name=='<Insira seu E-Mail Aqui>'].{Name:name, ID:id, Default:isDefault}" -o table
```

4 - Utilizar a sua assinatura do Azure (troque o "Sua Assinatura" abaixo pelo nome da sua assinatura da conta Azure).

```sh
az account set --subscription "<Sua Assinatura>"
```

5 - Consultar o nome do Resource Group criado para a sua conta.

```sh
az group list -o table
```

6 - Ajustar a variável _resource_group_name_ do arquivo `adls-azure-main/variables.tf` com o nome do Resource Group informado no passo anterior.

```terraform
variable "resource_group_name" {
  default = "<Seu Resource Group>"
}
```

7 - Criar os recursos na assinatura Azure selecionada.

```sh
terraform init
```

```sh
terraform validate
```

```sh
terraform fmt
```

```sh
terraform plan
```

```sh
terraform apply
```

8 - Logar no [portal.azure.com](https://portal.azure.com/) e conferir o deploy do ADLS.

## DataBricks

1 - Dentro do Portal Azure, criar um recurso DataBricks.

2 - Entre no Workspace Databricks, vá na aba computação e crie um Cluster.

3 -
