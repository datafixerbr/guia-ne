# Guia Nordeste de Fontes Jornalísticas 🌵📊

O **Guia Nordeste de Fontes Jornalísticas** é uma iniciativa do projeto
de extensão **O Moderno Popular**, realizado pelo **Observatório da Vida
Agreste (OVA)** da **Universidade Federal de Pernambuco (UFPE)**.

Este repositório contém a infraestrutura de dados responsável por
extrair, tratar e categorizar informações de pesquisadores das
universidades públicas federais nordestinas, combatendo a
sub-representação da região na mídia nacional.

## 📑 Sumário

-   Contexto e Justificativa\
-   Objetivos do Projeto\
-   Arquitetura de Dados\
-   Tecnologias Utilizadas\
-   Como Executar\
-   Impacto Social e ODS\
-   Créditos e Realização\
-   Contribuição

## 🔍 Contexto e Justificativa

Historicamente, o Nordeste brasileiro é retratado pela mídia comercial
sob estereótipos de "tradicionalismo" ou "terra de carências", excluindo
os profissionais da região do lugar da racionalidade e da expertise
técnica.

Pesquisas indicam que apenas **12% das fontes utilizadas por jornalistas
no Twitter** (frequentemente usado como banco de fontes) estão
localizadas nas regiões Norte e Nordeste.\
Este projeto visa romper essa barreira, fornecendo um banco de dados
robusto de especialistas localizados tanto nas capitais quanto no
interior do Nordeste.

## 🎯 Objetivos do Projeto

-   **Visibilização:** Catalogar especialistas das áreas de saúde,
    comunicação, arte, tecnologia e ciências humanas/sociais das UFs
    nordestinas.\
-   **Diversidade:** Priorizar o levantamento de dados sobre raça,
    gênero, território e pessoas com deficiência (PCDs) para promover
    uma pluralidade real de vozes.\
-   **Democratização:** Facilitar o acesso de jornalistas, ONGs e
    pesquisadores à produção científica de excelência do Nordeste.

## 🏗 Arquitetura de Dados

O pipeline de dados deste repositório segue a lógica de extração da
**Plataforma Lattes** para identificar o perfil dos docentes
pesquisadores:

-   **Extração (ETL):** Scripts para coleta automatizada de dados
    públicos de pesquisadores vinculados às instituições nordestinas.\
-   **Transformação:** Limpeza de dados, normalização de áreas de
    atuação e enriquecimento com informações coletadas via formulários
    suplementares.\
-   **Categorização:** Agrupamento por especialidades e subáreas de
    interesse para facilitar a busca jornalística.

## 🛠 Tecnologias Utilizadas

-   **Python:** Linguagem principal para o processamento de dados.\
-   **Pandas / Polars:** Manipulação e análise de grandes volumes de
    dados de pesquisadores.\
-   **BeautifulSoup / Selenium:** (Se aplicável) Extração de metadados
    de portais institucionais e Lattes.\
-   **GitHub Actions:** Automação do pipeline de atualização.

## 🚀 Como Executar

> **Nota:** Ajuste os comandos conforme a estrutura real do seu
> repositório.

### Clone o repositório

``` bash
git clone https://github.com/datafixerbr/guia-ne.git
```

### Instale as dependências

``` bash
pip install -r requirements.txt
```

### Execute o script de extração

``` bash
python src/extraction/lattes_crawler.py
```

## 🌍 Impacto Social e ODS

O Guia está alinhado com quatro **Objetivos de Desenvolvimento
Sustentável (ODS)** da ONU:

-   **ODS 4 --- Educação de Qualidade:** Valorização das universidades
    federais.\
-   **ODS 5 --- Igualdade de Gênero:** Visibilidade para pesquisadoras.\
-   **ODS 10 --- Redução das Desigualdades:** Combate ao elitismo
    regional (eixo Sul--Sudeste).\
-   **ODS 16 --- Paz, Justiça e Instituições Eficazes:** Fortalecimento
    de um ambiente democrático e menos xenófobo.

## 👥 Créditos e Realização

O projeto é coordenado pela **professora Fabiana Moraes da Silva** no
âmbito do **Observatório da Vida Agreste (OVA)** e do curso de
**Comunicação Social da UFPE**.

-   **Parceria:** Coletivo jornalístico *Marco Zero Conteúdo*.\
-   **Homenagem:** Inspirado pelo projeto *Entreviste um Negro*, de
    Helaine Martins.

## 🤝 Contribuição

Sinta-se à vontade para abrir uma **issue** ou enviar um **pull
request**.\
Como produzimos ciência nacional, sua colaboração ajuda a universalizar
o conhecimento produzido no Nordeste.


