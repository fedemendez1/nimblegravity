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

- **Repo:** `fedemendez1/nimblegravity`, carpeta `fmcg_mmm/`. **Branch vigente: `claude/sharp-lamport-uc26sg`** (parte de `claude/friendly-gates-a0mnhs` + sesión 2 de modelado). Leer siempre la más reciente.
- **Dashboard (artifact privado):** https://claude.ai/artifact/5Z4CppFB4yJPW2MYC4xThA. Se regenera con `python src/05_dashboard.py` y se republica desde `outputs/dashboard.html`.
- **Entorno:** `.venv` en la raíz del repo (no versionado). `pip install -r fmcg_mmm/requirements.txt` (Meridian 2.1.0). Correr los scripts desde `fmcg_mmm/src`.
- **Modelos:** `*.pkl` está en `.gitignore`, pero **desde la sesión 2 se versionan con `git add -f`** (~3 MB c/u) para no volver a correrlos. En el repo: `mmm_f1.pkl` (base, screen), `mmm_cC.pkl`, `mmm_cCA.pkl`. El base con cadenas completas (`mmm_base.pkl`, `_ho13`) NO está guardado: regenerar con `03_model.py` (~12 min) y `--holdout 13`.
  - Variantes de sensibilidad: `--draws screen` (~4 min). Nuevo arg `--extra col1 col2` para agregar drivers sin tocar `DRIVERS`.
  - **Regla:** cada modelo nuevo que corramos → `git add -f` del `.pkl` + tablas, commit y push en el momento.

### Pipeline

| Script | Qué hace |
|---|---|
| `01_clean.py` | Checks y dataset limpio `data/clean/model_data.csv` |
| `02_eda.py` | Gráficos `eda_*` y tablas `eda_*` |
| `03_model.py` | Fit de Meridian. Args: `--tag --holdout --knots --roi-prior mu,sigma --draws quick/screen/full --no-season` |
| `04_diagnostics.py` | Convergencia, fit, residuos, signos, elasticidad, ROI, contribuciones y curvas de respuesta → `outputs/tables`, `outputs/figures` |
| `05_dashboard.py` + `dashboard_template.html` | Dashboard HTML |
| `06_sensitivity.py` | Compara variantes → `sens_*.csv` y su figura |
| `07_residuals.py --model mmm_x` | Residuos semanales, ACF/DW por año, screening de todas las columnas crudas y lags vs. residuos, figura `diag_02_residual_autocorr_<model>` |

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

## Sesión 2 de modelado (oct-2026): precio, distribución, autocorrelación

El usuario NO quiere pasar a storytelling hasta cerrar el modelado. Explicar todo sin jerga (pidió explicaciones "desde el principio").

### Precio – cerrado (como caveat, no como problema)
- Nuestro precio/kg subió ~50% (£0.70→£1.05), competencia ~10%. Premium 1.4×→2.0× casi lineal: corr con el tiempo 0.91, solo ~5% de la varianza es de corto plazo.
- Volumen plano (corr con tiempo 0.02); distribución subió (~41→45%, con caída a ~36% a fines de 2022 y salto a ~46% a mediados de 2024).
- Lectura: premium resta ~15% volumen, distribución suma ~12–13% → se compensan. Elasticidad −0.48 (90% −0.58/−0.39), P(<0)≈100%, sigue lejos de 0 con intervalos ×1.4. Inelástico → la suba de precio aumentó la facturación.
- Caveats acordados: (1) depende de "sin tendencia oculta" (6 knots la anula; 1 knot defendido por parsimonia + holdout); (2) es elasticidad relativa, no propia; (3) el −16% es vs. el premium mínimo observado (1.26×).
- Decidimos NO correr la variante con tendencia lineal.

### Distribución
- Es ACV ponderada (facturación total de la tienda, todas las categorías). El usuario objetó que lo relevante sería la ponderada por categoría (PCV): no está en la data, solo ACV. Caveat menor (ruido de medición; posible endogeneidad: los retailers listan lo que vende).
- ~+3% volumen por punto de distribución (aprox.).

### Autocorrelación – en curso
- Residuos base (`mmm_f1`): ACF lag1 0.35, **DW 1.26**. Por año: 2022 0.48 (DW 0.94), 2023 0.05 (DW 1.89), 2024 0.32 (DW 1.35). No es solo la primavera de 2022 (sin ella: 0.28). Son desvíos de nivel de meses: inicio 2022 (−), mar–may 2022 (+), ene–may 2024 (−).
- Screening de las 89 columnas crudas + lags de drivers vs. residuos: el único candidato con sentido es la **promo de la marca C** (share de volumen en promo, corr 13w −0.36, máx corr con drivers 0.37).
- Variantes (screen, tablas `sens_comp_promo*.csv`):

| | base f1 | + promo C (cC) | + promo C + desc. A (cCA) |
|---|---|---|---|
| R² | 0.914 | 0.922 | 0.923 |
| ACF lag1 / DW | 0.35 / 1.26 | 0.31 / 1.34 | 0.31 / 1.34 |
| Elasticidad precio | −0.48 | −0.46 | −0.46 |
| ROI total | 1.15 | 1.11 | 1.10 |

  - `comp_c_promo_share`: −0.10 (90% −0.15/−0.05), signo OK → **propuesta: incluirla en el modelo final**. `comp_a_promo_depth` no significativa → descartar.
- Rescreening sobre residuos de `cC`: nada más con señal (todo <0.25; RDM −0.38 es espurio).
- El usuario prefiere no presentar la autocorrelación "así" y está dispuesto a sumar variables aunque suba el VIF (en Nielsen solo sacaban si corr >0.9).

### Próximo paso acordado (pendiente de correr)
1. `03_model.py --tag cC_k2 --knots 2 --draws screen --extra comp_c_promo_share` y lo mismo con `--knots 3`. Mirar ACF/DW, elasticidad de precio (¿sobrevive?) y holdout.
2. Si el precio sobrevive y baja la ACF → ese es el modelo final; si no → final = base + promo C, y presentar la autocorrelación como diagnóstico trabajado (DW ~1.3, intervalos ×1.4).
3. Dummies de período (inicio 2022, H1 2024) solo si hay razón de negocio.
4. Después: correr el final con cadenas completas + holdout 13, `04_diagnostics.py`, `07_residuals.py`, `05_dashboard.py`, actualizar README. Recién ahí storytelling.
5. Otras decisiones abiertas (de la sesión 2, aún sin discutir): ROI por canal vs. solo total (canales no identificados; ¿agrupar en 2–3?); mostrar o no curvas de respuesta (dependen del prior); promo propia ≈0; base de contribuciones (vs. mínimo observado → base 71%; agrupar estilo Nielsen base = fondo + precio + distribución + clima); umbral 20°C de heat_excess sin sensibilidad.

## Pendiente / próximos pasos

1. **Storytelling y presentación** (después de cerrar el modelado, ver arriba): 5–6 slides, 15–20 min, client-facing, con respaldo técnico (appendix).
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
