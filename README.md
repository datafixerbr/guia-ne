# Análise Estatística de Currículos Lattes

## Visão Geral

Este projeto implementa uma metodologia estatisticamente robusta para análise de uma base de dados massiva de currículos Lattes (7,3 milhões de arquivos), utilizando amostragem representativa ao invés de processamento completo da população. A abordagem reduz drasticamente o tempo de processamento (de dias para minutos) mantendo 95% de confiança estatística.

## 📊 Metodologia Estatística

### Cálculo da Amostra Representativa

Utilizamos a **Fórmula de Cochran** ajustada para população finita:

```
n₀ = (Z² × p × (1-p)) / e²
n = n₀ / (1 + ((n₀ - 1) / N))
```

**Onde:**

- `Z = 1,96` (95% de confiança)
- `p = 0,5` (proporção conservadora)
- `e = 0,05` (±5% margem de erro)
- `N = 7.391.139` (população total)

**Resultado:** 500 arquivos representam estatisticamente toda a base com 95% de confiança.

### Método de Amostragem

- **Amostragem sistemática** com ponto de partida aleatório
- **Seed fixa (42)** para reprodutibilidade
- **Intervalo:** 14.782 (N/n)
- **Representatividade:** 0,007% da população total

## 🛠️ Estrutura do Projeto

```
projeto/
├── data/
│   ├── lattes_amostra_metadata.csv      # Metadados da amostra (485 registros)
│   ├── arquivos_amostra.txt             # Lista dos arquivos selecionados
│   ├── xml_files/                       # XMLs extraídos organizados por ZIP
│   └── amostragem_lattes.log            # Log do processamento
├── extrator.py                          # Script de amostragem e metadados
├── motor_metadados.py                   # Script de extração de XMLs
└── nbs                                  # Diretório para Notebook de análise
    └── cnpq_lattes_discovery.ipynb      # Notebook de análise dos metadados
```

## 🚀 Scripts Principais

1. `motor_metadados.py` - Gerador de Metadados da Amostra

    Funcionalidades

    - Calcula amostra estatisticamente representativa
    - Extrai metadados de estrutura dos XMLs
    - Monitora recursos do sistema
    - Sistema de checkpoint para recuperação

2. `extrator.py` - Extrator de XMLs da Amostra

    Funcionalidades:

    - Extrai arquivos XML apenas da amostra selecionada
    - Organiza em subdiretórios por arquivo ZIP
    - Processa de forma eficiente e segura

## 📈 Análise Exploratória

### Dados Coletados

O dataset `lattes_amostra_metadata.csv` contém:

| Campo | Descrição |
| :-- | :-- |
| `arquivo_zip` | Nome do arquivo ZIP |
| `xml_arquivo` | Nome do arquivo XML |
| `linhas` | Número de linhas do currículo |
| `colunas` | Número de campos preenchidos |
| `zip_tamanho_mb` | Tamanho do arquivo comprimido |
| `xml_tamanho_kb` | Tamanho do XML descomprimido |
| `encoding_detectado` | Codificação do arquivo |
| `elementos_xml_raiz` | Elemento raiz da estrutura XML |
| `status` | Status do processamento |

### Indicadores Principais Identificados

**Qualidade dos Dados:**

- 99,79% de taxa de sucesso no processamento
- Estrutura XML consistente (CURRICULO-VITAE)
- Encoding predominante: ISO-8859-1

**Padrões Temporais:**

- Concentração de atualizações em julho
- Maior atividade em dias úteis
- Padrão reativo de atualização

**Estrutura dos Currículos:**

- 83% dos currículos têm 5 linhas (estrutura padrão)
- Média de 20 campos preenchidos por currículo
- Variação de 14 a 37 campos por registro

## 🔧 Requisitos e Instalação

### Dependências

```bash
pip install pandas numpy matplotlib seaborn psutil
```

## 📊 Resultados Estatísticos

### Projeções para População Total

Com base na amostra de 485 registros válidos:

| Métrica | Valor Projetado |
| :-- | :-- |
| Currículos processáveis | ~7.360.000 |
| Total de linhas estimadas | ~31.000.000 |
| Campos médios por currículo | 20,1 |
| Espaço total estimado | ~55 GB |
| Taxa de processamento | 99,8% |

**Intervalo de Confiança:** 95% ± 5%

## ⚠️ Limitações e Considerações

### Limitações da Amostra

- **Eventos raros** (<0,1%) podem não ser capturados
- **Análises muito específicas** de subgrupos pequenos
- **Sazonalidade** concentrada em julho de 2024

## 📚 Referências Metodológicas

- **Cochran, W.G.** (1977). *Sampling Techniques*. 3rd Edition, John Wiley \& Sons
- **Lohr, S.L.** (2010). *Sampling: Design and Analysis*. 2nd Edition, Brooks/Cole
- Metodologia aplicada em grandes bases de dados do IBGE e censos internacionais

## 🤝 Contribuições

Para melhorias no projeto:

1. **Validação cruzada** com diferentes sementes
2. **Amostragem estratificada** por área de conhecimento
3. **Análise temporal** em diferentes períodos
4. **Otimizações de performance** para hardware específico

<div style="text-align: center">⁂</div>


