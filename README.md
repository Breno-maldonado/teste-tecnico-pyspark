# Teste Técnico - Engenheiro de Dados PySpark

Este projeto realiza a análise de dados de pedidos e clientes de um e-commerce utilizando Apache Spark (PySpark). O objetivo é transformar dados brutos em insights sobre o comportamento de compra, tratando anomalias e calculando métricas estatísticas.

## 🚀 Tecnologias Utilizadas
- **Python 3.12**
- **Apache Spark 3.5.0** (PySpark)
- **Hadoop 3.3** (Winutils para ambiente Windows)

## 📊 Estrutura do Projeto
- `main.py`: Script principal contendo toda a lógica de ETL e análise.
- `dados/`: Pasta contendo as fontes de dados JSON.
  - `clients/`: Dados cadastrais dos clientes.
  - `pedidos/`: Histórico de pedidos.

## 🛠️ Soluções Implementadas

1.  **Data Quality**: Identificação de registros inconsistentes (IDs nulos ou valores menores/iguais a zero) com relatório de motivos.
2.  **Performance (Broadcast Join)**: Como a tabela de clientes é significativamente menor que a de pedidos, foi utilizado o `F.broadcast()` para otimizar o join, evitando o embaralhamento de dados (shuffle) na rede.
3.  **Análise Estatística**: Cálculo de média, mediana e percentis (P10 e P90) para compreensão da distribuição de gastos.
4.  **Tratamento de Outliers**: Implementação de filtragem por média truncada, removendo os extremos (10% inferiores e 10% superiores) para uma visão mais realista do faturamento recorrente.

## 📋 Como Executar

1. **Pré-requisitos**:
   - Ter o Java JDK 11 ou 8 instalado.
   - Instalar as dependências: `pip install pyspark`.
   - (Apenas Windows) Ter o `winutils.exe` e `hadoop.dll` configurados na pasta `C:\hadoop\bin`.

2. **Execução**:
   No terminal, dentro da pasta do projeto, execute:
   ```bash
   python main.py


## OBS

Desde já agradeço pela oportunidade, pedi para o GPT escrever esse README porque fica mais bonitinho, e também usei ele para resolver o bug do meu win que não estavam querendo rodar o spark kkkk, mas o codigo esta limpo, 100% a mão!
