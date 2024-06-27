# Pre requisitos para executar o projeto 

Segue os pre requisitos para execução do projeto.

- Será necessário uma conta na Microsoft Azure.

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

## SQL Server 

1 - Dentro do portal Azure, criar um banco SQL do Azure

2 - Executar o script de criação das tabelas no seu banco de dados. 

```
CREATE TABLE "comanda"(
    "id_comanda" INT NOT NULL,
    "id_mesa" INT NOT NULL,
    "id_pagamento" INT NOT NULL,
    "id_funcionario" INT NOT NULL,
    "valor_total" FLOAT NOT NULL
);
ALTER TABLE
    "comanda" ADD CONSTRAINT "comanda_id_comanda_primary" PRIMARY KEY("id_comanda");



CREATE TABLE "pagamento"(
    "id_pagamento" INT NOT NULL,
    "tipo" VARCHAR(25) NOT NULL,
    "status" BIT NOT NULL,
    "taxa" FLOAT NOT NULL
);
ALTER TABLE
    "pagamento" ADD CONSTRAINT "pagamento_id_pagamento_primary" PRIMARY KEY("id_pagamento");


CREATE TABLE "ingredientes"(
    "id_item_cardapio" INT NOT NULL,
    "id_estoque" INT NOT NULL,
    "quantidade" INT NOT NULL,
    "unidade" VARCHAR(10) NULL
);



CREATE TABLE "estoque"(
    "id_estoque" INT NOT NULL,
    "ingrediente" VARCHAR(30) NOT NULL,
    "quantidade" FLOAT NOT NULL,
    "unidade" VARCHAR(10) NULL
);
ALTER TABLE
    "estoque" ADD CONSTRAINT "estoque_id_estoque_primary" PRIMARY KEY("id_estoque");




CREATE TABLE "funcionarios"(
    "id_funcionario" INT NOT NULL,
    "nome" VARCHAR(20) NOT NULL,
    "sobrenome" VARCHAR(50) NOT NULL,
    "telefone" VARCHAR(30) NOT NULL,
    "email" VARCHAR(50) NULL,
    "cargo" VARCHAR(20) NOT NULL,
    "data_contratacao" DATETIME NOT NULL
);
ALTER TABLE
    "funcionarios" ADD CONSTRAINT "funcionarios_id_funcionario_primary" PRIMARY KEY("id_funcionario");

CREATE TABLE "cardapio"(
    "id_item_cardapio" INT NOT NULL,
    "nome_item" VARCHAR(30) NOT NULL,
    "valor" FLOAT NOT NULL,
    "descricao" VARCHAR(80) NOT NULL,
    "categoria" VARCHAR(30) NOT NULL,
    "disponibilidade" BIT NOT NULL
);
ALTER TABLE
    "cardapio" ADD CONSTRAINT "cardapio_id_item_cardapio_primary" PRIMARY KEY("id_item_cardapio");
CREATE INDEX "cardapio_nome_item_index" ON
    "cardapio"("nome_item");



CREATE TABLE "mesas"(
    "id_mesa" INT NOT NULL,
    "qtd_lugares" INT NOT NULL,
    "local" VARCHAR(100) NOT NULL,
    "status" BIT NOT NULL
);
ALTER TABLE
    "mesas" ADD CONSTRAINT "mesas_id_mesa_primary" PRIMARY KEY("id_mesa");



CREATE TABLE "pedido"(
    "id_pedido" INT NOT NULL,
    "id_comanda" INT NOT NULL,
    "id_cardapio" INT NOT NULL,
    "status" BIT NOT NULL,
    "data_hora_pedido" DATETIME NOT NULL,
    "quantidade" INT NOT NULL
);

ALTER TABLE
    "pedido" ADD CONSTRAINT "pedido_id_pedido_primary" PRIMARY KEY("id_pedido");

ALTER TABLE
    "comanda" ADD CONSTRAINT "comanda_id_funcionario_foreign" FOREIGN KEY("id_funcionario") REFERENCES "funcionarios"("id_funcionario");
ALTER TABLE
    "comanda" ADD CONSTRAINT "comanda_id_mesa_foreign" FOREIGN KEY("id_mesa") REFERENCES "mesas"("id_mesa");
ALTER TABLE
    "comanda" ADD CONSTRAINT "comanda_id_pagamento_foreign" FOREIGN KEY("id_pagamento") REFERENCES "pagamento"("id_pagamento");
ALTER TABLE
    "ingredientes" ADD CONSTRAINT "ingredientes_id_item_cardapio_foreign" FOREIGN KEY("id_item_cardapio") REFERENCES "cardapio"("id_item_cardapio");
ALTER TABLE
    "ingredientes" ADD CONSTRAINT "ingredientes_id_estoque_foreign" FOREIGN KEY("id_estoque") REFERENCES "estoque"("id_estoque");
ALTER TABLE
    "pedido" ADD CONSTRAINT "pedido_id_cardapio_foreign" FOREIGN KEY("id_cardapio") REFERENCES "cardapio"("id_item_cardapio");
ALTER TABLE
    "pedido" ADD CONSTRAINT "pedido_id_comanda_foreign" FOREIGN KEY("id_comanda") REFERENCES "comanda"("id_comanda");
```

3 - Executar o codigo `./PythonFaker/faker.ipynb` alterando as seguintes variaveis para as respectivas variaveis do seu banco.
``` 
# Definir os detalhes da conexão
server = ''
database = ''
username = ''
password = ''
```

## DataBricks

1 - Dentro do Portal Azure, criar um recurso DataBricks.

2 - Entre no Workspace Databricks, vá na aba computação e crie um Cluster.

3 - Na aba espaço de trabalhos, conecte o databricks com o repositório no GitHub. 

4 - No Notebook `Notebooks Databricks/Landing/LakeHouse - Landing.py` Alterar as variaveis (Host, Database, Username, Password) para os dados do seu banco.
```
Host = ""
Port = 1433
Database = ""
Username = ""
Password = ""
Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
Url = f"jdbc:sqlserver://{Host}:{Port};databaseName={Database}"
```

5- Em todos os Notebooks alterar as seguintes variaveis para os dados do seu Azure Data Lake storage.
```
storageAccountName = ""
storageAccountAccessKey = ""
sasToken = ""
```

## Ir para ...

- -> **[Apresentação](./index.md)**.
- -> **[Pré-Requisitos e Ferramentas](./prerequisitos.md)**.
- -> **[Execução](./comoExecutar.md)**.
