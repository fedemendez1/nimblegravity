# Project context – FMCG MMM (entrevista Director de MMM)

Archivo para retomar el trabajo en una sesión nueva. Leer junto con `README.md` (definición del modelo y resultados en inglés).

## Objetivo y encuadre

- **Consigna** (`data/raw/task_brief.pdf`): agencia Adclaro, cliente FMCG de agua embotellada. Hay que:
  - explorar la data;
  - construir un modelo simple;
  - preparar una presentación de **5–6 slides, 15–20 min**, client-facing pero con respaldo técnico.
- **Lo que piden cubrir:** data overview, impacto y efectividad de la media, otras variables influyentes.
- **Scope acordado:** trabajo equivalente a ~4 h. Criterio 80/20; lo que no entra va a la sección "With more time". No hacerles toda la tarea gratis.

## Preferencias del usuario (respetar)

- **Conversación:** en español. Respuestas cortas, que contesten la pregunta, sin "biblias".
- **Código, comentarios y outputs:** en inglés (la agencia es UK, £).
- **Código:** conciso, eficiente, que parezca escrito por un humano. Pocos comentarios.
- **Trabajo:** pragmático, enfocado en delivery. Debatir decisiones con el usuario antes de cambios grandes.
- **Background:** viene de R y conoce MMM bayesiano con priors. Aceptó Python + Meridian. No es experto en git/VS Code: darle pasos claros.

## Dónde está todo

- **Repo:** `fedemendez1/nimblegravity`, branch `claude/friendly-gates-a0mnhs`, carpeta `fmcg_mmm/`.
- **Dashboard (artifact privado):** https://claude.ai/artifact/5Z4CppFB4yJPW2MYC4xThA. Se regenera con `python src/05_dashboard.py` y se republica desde `outputs/dashboard.html`.
- **Entorno:** `.venv` en la raíz del repo (no versionado). `pip install -r fmcg_mmm/requirements.txt` (Meridian 2.1.0). Correr los scripts desde `fmcg_mmm/src`.
- **Modelos:** los `.pkl` no se versionan. Hay que regenerarlos:
  - `03_model.py` (base, ~12 min en 4 CPU) y `03_model.py --holdout 13`;
  - las variantes de sensibilidad usan `--draws screen` (~4 min).

### Pipeline

| Script | Qué hace |
|---|---|
| `01_clean.py` | Checks y dataset limpio `data/clean/model_data.csv` |
| `02_eda.py` | Gráficos `eda_*` y tablas `eda_*` |
| `03_model.py` | Fit de Meridian. Args: `--tag --holdout --knots --roi-prior mu,sigma --draws quick/screen/full --no-season` |
| `04_diagnostics.py` | Convergencia, fit, residuos, signos, elasticidad, ROI, contribuciones y curvas de respuesta → `outputs/tables`, `outputs/figures` |
| `05_dashboard.py` + `dashboard_template.html` | Dashboard HTML |
| `06_sensitivity.py` | Compara variantes → `sens_*.csv` y su figura |

## Decisiones tomadas (y por qué)

1. **KPI:** `Volume_Sales` (kg), con `revenue_per_kpi = Avg_PPKG`. El precio queda como driver y el ROI sale en £.
2. **Herramienta:** Meridian (pedido del usuario). Adstock geométrico + Hill (Weibull no existe en Meridian; el usuario lo aceptó).
3. **Ejecución de media:** impresiones/GRPs, con el gasto solo como denominador del ROI.
   - El CPM de Social varía 3× (CV 29%) y el de RDM tiene CV 93%.
   - TV va separado de digital video: GRPs e impresiones no se suman, y el CPM de TV (£2) vs. VOD (£29) haría que TV domine el grupo.
   - Partnership y OOH quedan en gasto porque no tienen otra medida.
   - Canales: `tv, digital_video (VOD+OLV), social, partnership, ooh, search_rdm`.
4. **Prior de ROI:** LogNormal(0, 0.7), mediana 1, igual para todos los canales. La sensibilidad lo valida (ver resultados).
5. **Drivers** (`non_media_treatments`, prior de coeficiente N(0,5)): temp_avg, heat_excess, distribution, log_rel_price, promo_intensity, comp_media_spend.
   - `heat_excess = max(tmax − 20, 0)`: capta los picos de ola de calor. Con ella el R² pasó de 0.81 a 0.90.
   - `log_rel_price`: log del precio base propio vs. el de competencia. Precio propio y de competencia correlacionan 0.86, así que no se pueden separar.
   - `promo_intensity`: profundidad × amplitud. La profundidad sola salía con el signo invertido y es colineal con la amplitud.
6. **Controles:** rainfall, new_year_week (−20% todos los años) y un par de Fourier anual. Fourier cubre que, a igual temperatura, la primavera vende más que el otoño.
7. **Línea base:** 1 knot.
   - Con 6 knots la elasticidad de precio cae a 0 (la línea base absorbe el precio).
   - Con 12 knots el holdout MAPE sube a 34% (sobreajuste).
8. **Descartadas a propósito:** promo share (derivada del outcome) y volumen de competencia (endógeno).

## Resultados del modelo final (`mmm_base`)

### Diagnóstico

- **Convergencia:** R-hat ≤ 1.003, 0 divergencias.
- **Ajuste:** R² 0.91, MAPE 4.4%. **Holdout** (últimas 13 semanas): MAPE 4.0%.
- **Residuos:** autocorrelación lag-1 de 0.35, que no se resuelve.
  - Meridian no soporta errores AR.
  - Efecto: los intervalos reales son ~1.4× más anchos. Hay que mencionarlo.

### Hallazgos

- **Media:** ~0.8% del volumen. Se gastaron £3.5M en 3 años (0.8% de las ventas en valor).
- **ROI total:** **1.13** (0.69–1.80), en ingresos y antes de margen.
- **ROI por canal:** no identificados por la data.
  - El R² es idéntico con cualquier prior (0.914–0.915) y los ROIs se mueven con él.
  - El ROI total es robusto: 1.06–1.16 con priors de mediana 1; 1.75 con prior centrado en 2.
- **TV:** único canal que la data empuja hacia abajo. ROI 0.72, P(ROI>1) = 26%; queda último también con el prior ancho.
- **Elasticidad de precio** al premium vs. competencia: **−0.48** (−0.58 a −0.39). El premium (~1.8×) cuesta ~16% del volumen.
- **Contribuciones** (vs. el nivel mínimo observado de cada driver):
  - distribución +27%
  - temperatura +10%, más calor +6%
  - precio −16%
  - promo ≈ 0
  - media de competencia ≈ 0. Antes salía positiva, pero era estacionalidad mal atribuida.
- **Curvas de respuesta:** todos los canales están saturados, con ROI marginal ~0.4–0.8.

### Historia propuesta

- El volumen lo mueven distribución, clima y precio.
- La media es chica y en conjunto se paga apenas.
- La data no alcanza para rankear canales: recomendar lift/geo tests antes de redistribuir presupuesto. TV es el candidato más claro a revisar.

## Pendiente / próximos pasos

1. **Storytelling y presentación** (próximo paso acordado): 5–6 slides, 15–20 min, client-facing, con respaldo técnico (appendix).
   - Mensajes y orden a definir con el usuario.
   - Usar los gráficos de `outputs/figures` o el dashboard.
2. **Sección "With more time"** (ya en el README):
   - calibración con lift tests;
   - modelo geo;
   - ROI sobre margen;
   - elasticidad propia vs. cruzada;
   - errores AR en PyMC/Stan;
   - optimizador de presupuesto.

## Gotchas técnicos

- **Priors en Meridian 2.1:** se exigen en float64. Usar `backend.tfd.LogNormal(np.float64(...))`.
- **`save_meridian` (serde)** requiere `google-meridian[schema]`. Usamos `model.save_mmm` (pickle, deprecado pero funciona).
- **Pip:** pip del sistema falla (paquetes de Debian). Usar el venv.
- **Background:** `pkill -f 03_model.py` mata el propio shell si el patrón está en el comando. Evitarlo.
- **Gráficos:** paleta de `style.py`, una sola escala por eje.
