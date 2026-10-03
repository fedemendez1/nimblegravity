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
| `08_ar_twin.py [--no-ar]` | Gemelo bayesiano (PyMC) de la spec de Meridian con errores AR(1) estimados en conjunto. ~15 s → `tables/ar_twin.csv` |
| `09_variable_search.py` | Búsqueda de variables con modelo espejo lineal (replica residuos de Meridian, corr 0.99) → `tables/search_candidates.csv` |
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
- **Mensaje refinado (sesión 3):** suba del premium 1.4×→2.0× (Δlog 0.36) ⇒ volumen ~−15% *contrafactual* ("sin la suba habríamos vendido ~15% más"; el volumen real quedó plano porque la distribución compensó). Precio propio +50% ⇒ facturación por efecto precio ~+27%. Robusto en todo el intervalo AR (−0.61/−0.32): volumen −20%/−11%, facturación +20%/+34%; aun con elasticidad −1 sería ~+5%.
  - Decir "facturación", no "rentabilidad" (no hay márgenes/costos; 2022–23 hubo inflación de costos).
  - No mezclar con el −16% de contribuciones (base = premium mínimo 1.26×, no el inicial). Usar una sola base en la slide.
  - Cálculos hechos a mano con la elasticidad; si van a slide, sacarlos del modelo.

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

### Autocorrelación – RESUELTA (sesión 3)
**Búsqueda exhaustiva de variables** (`09_variable_search.py`, modelo espejo OLS con media ≥0 que replica residuos de Meridian; 114 candidatas curadas + ~250 crudas, individuales y forward por BIC/ACF):
- Ninguna variable legítima baja la ACF más allá de promo C (0.31). Lo que la baja más tiene signo absurdo (premium vs A positivo, distribución de B positiva, profundidad de promo B/C positiva) o es endógeno.
- Descartadas con razón: `Number_of_Stores_Selling` (corr 0.74 con volumen, 0.43 con temperatura → refleja demanda; la ACV usada tiene 0.00 con temperatura); media de A (31 semanas, casi todo Q2–Q3 2024 = dummy disfrazada); volúmenes de competidores (endógenos).
- Índice de precio de competencia: alternativas (pesos fijos, por marca, por unidad) no cambian la ACF.
- Clima no lineal (tramo 15–20°C, tmin ≈ sol) sube R² a 0.955 pero no baja ACF y empeora holdout → no se incluye.
- Hallazgo: parte de la ACF en Meridian la induce el prior de ROI (media forzada ≥0; con media libre el espejo "explica" la caída H1-2024 con RDM negativo).
- Conclusión: el resto son desvíos de demanda persistentes (meses) que ninguna columna explica → se **modela**, no se tapa.

**Solución en Meridian** (`03_model.py --ar-from mmm_cC`): GLS factible. El residuo de la semana anterior de una 1ª etapa (`mmm_cC`) entra como control `resid_lag1`; los residuos del modelo pasan a ser las innovaciones AR(1). Equivale a GLS-AR (verificado en espejo: converge en 1 iteración, ρ 0.32).

| (screen) | f1 | cC | **cCar** |
|---|---|---|---|
| ACF lag1 | 0.35 | 0.31 | **−0.02** (DW 2.01; 2022 0.09, 2023 −0.21, 2024 −0.04) |
| R² / MAPE | 0.914 / 4.4% | 0.922 / 4.3% | 0.930 / 4.0% (el R² sube por el término AR, no vender como mejora estructural) |
| Holdout MAPE (13 sem, cadena completa con holdout en ambas etapas) | – | 3.7% | 3.8% |
| Elasticidad | −0.48 | −0.46 | −0.47 (−0.56/−0.37) |
| ROI total | 1.15 | 1.11 | 1.09 |
ROIs por canal ~idénticos (TV 0.71 sigue último).

**Validación independiente** (`08_ar_twin.py`, AR(1) estimado en conjunto, no en 2 etapas): sin AR reproduce Meridian (ACF 0.31, elasticidad −0.46, ROI 1.09). Con AR: ρ 0.42 (0.28–0.56), ACF innovaciones −0.08, elasticidad −0.46 (−0.61/−0.32), ROI 1.07 (0.65–1.76). R-hat 1.004, 0 divergencias. → Puntos iguales; intervalos honestos ~1.4× más anchos en elasticidad (los de Meridian 2 etapas tratan el lag como conocido → usar los del gemelo para el ancho).

Mensaje para la entrevista: "Detectamos autocorrelación (DW 1.26), buscamos causas omitidas en todas las variables (solo promo C tenía sentido), la modelamos explícitamente como AR(1) dentro de Meridian y la validamos con un modelo bayesiano independiente: residuos limpios y conclusiones sin cambio."

**Decisión del usuario: se adopta la corrección AR(1)** (modelo final = drivers base + promo C + AR).

**Cómo explicarlo (sin "para arreglar la autocorrelación"):**
- El AR no es un driver: en promedio aporta 0 volumen y no le saca crédito a nada (prueba: elasticidad, ROI y distribución no cambian). Eso lo distingue de las variables "trampa" descartadas.
- Representa factores no observados que duran semanas: exhibiciones/espacio en góndola (ACV dice si estás, no cuánto), quiebres de stock, acciones de retailers/surtido, marcas propias (no están en la data), stockeo de packs. ρ≈0.4: un shock sigue ~40% la semana siguiente, ~16% la otra.
- Por qué hace falta: (1) sin él el modelo cree tener 161 semanas independientes, en realidad ~70 → intervalos sobreconfiados; (2) evita que shocks persistentes se los quede la variable con tendencia (precio, distribución).
- Argumento para el panel: es la versión acotada de la línea base variable de Meridian (knots), que es la solución de Google para lo mismo pero se comía el precio. Corrección AR es econometría estándar (Cochrane-Orcutt, 1949) y común en MMM econométrico clásico.
- Única trampa posible: vender el R² 0.93 como mejora (sale del término AR).

### Corridas del modelo final (lanzadas en sesión 3)
Tags: `stage1` (= cC, full) → `final` (`--ar-from mmm_stage1`, full); idem `_ho13`; sensibilidad de priors sobre el final (screen, `--ar-from mmm_stage1`): `final_wide` LogNormal(0,1.5), `final_tight` (0,0.35), `final_high` (0.693,0.7 = mediana 2) → `sens_priors_final.csv`. Nota: los parámetros exactos de wide/tight/high de la sesión 2 no estaban documentados; tight/high se reconstruyeron.

**Resultados (full, 4×1000):** R-hat ≤1.004, 0 divergencias. ACF −0.02 (DW 2.01), R² 0.93, MAPE 4.0%, holdout 3.8%. Elasticidad −0.47. ROI total 1.06 (0.65–1.66).

**Sensibilidad de priors sobre el final** (`sens_priors_final.csv`):

| Prior ROI | Total | TV | Video dig. | Social | Partner. | OOH | Search |
|---|---|---|---|---|---|---|---|
| base LN(0,0.7) | 1.06 | 0.69 | 0.91 | 0.98 | 0.91 | 1.34 | 0.99 |
| ancho LN(0,1.5) | 0.99 | 0.40 | 0.64 | 0.78 | 0.72 | 1.67 | 0.87 |
| angosto LN(0,0.35) | 1.04 | 0.89 | 1.00 | 1.01 | 0.99 | 1.09 | 1.01 |
| mediana 2 LN(0.69,0.7) | 1.61 | 1.00 | 1.40 | 1.56 | 1.42 | 2.33 | 1.97 |
R² idéntico (0.929–0.930), elasticidad −0.46/−0.48 en todas. Lectura: canales no identificados; patrón estable = TV último, OOH primero en los 4 priors; total ~1.0–1.06 con mediana 1 (data lo baja a 1.6 desde 2).

**Holdout: se reporta el de 26 semanas** (ago-24 → ene-25, pronóstico genuino al final de la serie). El de 13 semanas daba R² test 0.67 con MAPE 3.8% (igual que train) porque nov–ene es plano: sd 224k vs 673k de la serie, y el R² se mide contra la varianza del período. Espejo por bloques de 13 semanas: verano R² 0.81–0.86, invierno −1.3 a 0.4, MAPE 3–9% en todos → el R² de 13 semanas refleja la estación, no el modelo.
- `final_ho26`: **test R² 0.91, MAPE 4.6%** (train 0.93 / 4.1%). `stage1_ho26`: 0.915 / 4.1%.
- Estabilidad: entrenado sin los últimos 6 meses, elasticidad −0.46 (vs −0.47) y ROI total 1.13 (vs 1.06); TV 0.73 sigue último (`sens_ho26.csv`).
- R-hat explicado al usuario: chequeo de convergencia entre 4 cadenas (<1.01 OK); final ≤1.004, 0 divergencias. Una línea en el apéndice.

### Próximos pasos de modelado
1. ~~Revisar final full + holdout + sensibilidad de priors final.~~ Hecho. Luego `04_diagnostics.py --model mmm_final`, `05_dashboard.py` (apuntar a `mmm_final`), README (sacar AR de "With more time", agregar promo C y AR a la definición).
2. Decisiones abiertas: ROI por canal vs. total (¿agrupar en 2–3?), mostrar o no curvas de respuesta, base de contribuciones, sensibilidad umbral 20°C de heat_excess.
3. Knots 2/3: ya no hace falta.

### Próximo paso acordado en sesión 2 (superado por lo anterior)
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
   - optimizador de presupuesto.

## Gotchas técnicos

- **Priors en Meridian 2.1:** se exigen en float64. Usar `backend.tfd.LogNormal(np.float64(...))`.
- **`save_meridian` (serde)** requiere `google-meridian[schema]`. Usamos `model.save_mmm` (pickle, deprecado pero funciona).
- **Pip:** pip del sistema falla (paquetes de Debian). Usar el venv.
- **Background:** `pkill -f 03_model.py` mata el propio shell si el patrón está en el comando. Evitarlo.
- **Gráficos:** paleta de `style.py`, una sola escala por eje.
