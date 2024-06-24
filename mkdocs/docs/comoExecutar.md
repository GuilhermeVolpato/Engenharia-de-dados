# Como Executar


Executar o script da criação da tabela do seu banco de dados. 

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

Executar o codigo "./PythonFaker/faker.ipynb" alterando as seguintes variaveis para as respectivas variaveis do seu banco.
``` 
# Definir os detalhes da conexão
server = ''
database = 'Eng_Dados_Cafeteria'
username = ''
password = ''
```
Entrar no work space do DataBricks e rodar o workflow.

## Scripts que serão executados:

- Escrevendo dados do ambiente relacional na camada landing no formato delta.

```

```

- Passando os dados da landing para a camada bronze.

```

```

- Passando os dados da bronze para a camada prata.

```

```

- Passando os dados da camada prata para uma OBT na camada gold.

```

```







