# Vasco BI

Projeto de análise e projeção de desempenho do Vasco no Brasileirão usando dados da API football-data.org e BigQuery.

## 🚀 Pipeline

O pipeline é dividido em 3 etapas:

1. **Extract**
2. **Load**
3. **Transform**

---

## ⚙️ Setup

```bash
pip install -r requirements.txt
```

Configurar variáveis de ambiente:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=secrets/sua-credential.json
export GCP_PROJECT_ID=seu-project-id   # opcional
export BQ_DATASET=vasco_bi
export FOOTBALL_DATA_TOKEN=token
```

---

## ▶️ Execução

### 1. Extrair dados

```bash
python -m vasco_bi.etl.extract
```

### 2. Carregar no BigQuery

```bash
python -m vasco_bi.etl.load_bq
```

### 3. Transformar (todas as tabelas)

```bash
python -m vasco_bi.etl.transform --all
```

Ou steps específicos:

```bash
python -m vasco_bi.etl.transform --steps matches_enriched,fact_matches_running
```

---

## 🧠 Modelagem

Principais tabelas:

* `fact_matches_running` → histórico acumulado
* `agg_hist_*` → base histórica
* `agg_form_2026` → forma atual
* `agg_opponent_score_23_25` → dificuldade adversário
* `dim_future_matches_enriched` → features finais
* `fact_projection_matches` → projeção por jogo
* `fact_projection_micro` → projeção agregada

---

## 📊 Lógica de projeção

A projeção usa:

* Histórico (2023–2025)
* Forma atual (2026)
* Ajuste por dificuldade do adversário
* Conversão contínuo vs discreto (3/1/0)

---

## 📌 Observações

* Modelo heurístico (não ML)
* Otimizado para interpretabilidade
* Foco em decisão analítica (BI)
