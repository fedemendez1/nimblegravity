# Project context – FMCG MMM (entrevista Director de MMM)

Archivo para retomar el trabajo en una sesión nueva. **Leer primero "Estado actual"**; el resto es historia de decisiones (la información de sesiones anteriores que quedó superada está marcada). `README.md` tiene la definición del modelo y resultados en inglés.

## ESTADO ACTUAL (6-oct-2026) — leer primero

- **Fase:** modelado **CERRADO** (decisión del usuario). Estamos en **storytelling**: hay un MVP del deck para iterar slide por slide con el usuario.
- **Branch vigente:** `claude/upbeat-davinci-5zpud1` (sesión 5; contiene todo lo de `friendly-goldberg`). Ver "Sesión 5".
- **Deck (artifact Slides, privado):** https://claude.ai/artifact/Hr4ZPiahCBL7Jx26wFT2HG · fuentes en `fmcg_mmm/deck/project/`. Detalle en "Sesión 4c".
- **Dashboard técnico (artifact privado):** https://claude.ai/artifact/5Z4CppFB4yJPW2MYC4xThA · ya republicado con el modelo final (sesión 4).
- **Modelo final = `mmm_final`:** drivers base + promo de la marca C + corrección AR(1) (2 etapas: `mmm_stage1` → `mmm_final`). Holdout reportado: 26 semanas.

### Números vigentes (modelo final)
| Qué | Valor |
|---|---|
| Ajuste | R² 0.93, MAPE 4.0%; holdout 26 sem: R² 0.91, MAPE 4.6% |
| Convergencia | R-hat ≤ 1.004, 0 divergencias |
| Residuos | ACF −0.02, DW 2.01, Ljung-Box OK; colas/heterocedasticidad = ruido que escala con el nivel (caveat de apéndice) |
| VIF | máx 8.7 (temp_avg); precio 3.4, distribución 3.2, media 1.5–3.1 |
| Elasticidad al premium | −0.47 (−0.56/−0.38; −0.61/−0.32 con incertidumbre AR); rango en todas las variantes −0.41 a −0.50 |
| Distribución | +3.6% volumen por punto de ACV (3.2–4.0) |
| Calor | +4.1% volumen por °C de máxima semanal sobre 20°C (3.7–4.6) |
| Promo propia | ≈ 0: +1.7% por +10pp de intensidad (−1.9/+5.3) |
| Media | £3.5M en 3 años (0.8% de facturación), ~0.8% del volumen; **ROI total 1.06 (0.65–1.66)** en ingresos, corto plazo, antes de margen |
| ROI por campaña | 2023 TV+partnership 0.86 (0.42–1.58) · 2024 video+OOH 1.10 (0.49–2.19) · social 0.98 (0.32–2.60) |
| Sin restricciones (OLS estilo Nielsen) | total sin search 1.26 (0.13–2.40) → el ~1 lo da la data, no el prior |
| Año contra año | 2022→23 −8.2%: precio −7.7, competencia −1.8, clima −1.4, media +0.5 · 2023→24 +8.2%: distribución +13.3, precio −4.3, clima −1.6, competencia +1.5, media +0.7 |
| Hechos de negocio | Facturación £126M→£147M→£167M (+32%; 2022 tiene 53 semanas); volumen 167→154→166m kg; precio/kg £0.76→£1.00; premium 1.5×→1.9×; promo share 16%→33%; distribución 39%→43% (≈46% en verano 2024) |

### Qué NO afirmar (superado / no defendible)
- ~~"Todos los canales están saturados" / mROI 0.4–0.8~~: la saturación y el carryover son **100% prior** (posterior = prior). Con un solo nivel de presión por canal no se pueden estimar curvas. Curvas de respuesta solo en apéndice y como supuesto.
- ~~Ranking de canales~~: los canales que salieron juntos (TV dentro del flight de partnership en abr-23; video y OOH lanzados la misma semana en abr-24) no se pueden separar. Leer **por campaña**.
- ~~ROI 1.13 / elasticidad −0.48 / holdout 13 semanas~~: son del modelo base viejo (`mmm_base`). Usar los números de la tabla de arriba.
- No decir "rentabilidad" (no hay márgenes): decir "facturación/revenue".
- No vender el R² 0.93 como mejora estructural (parte viene del término AR).

### Próximos pasos
1. Iterar el deck slide por slide con el usuario (espera muchas vueltas). Abierto: dimensionar el plan (£ de +1 pt de distribución, de bajar promo); cómo decir lo de promo sin márgenes; waterfall en kg (eje truncado en 140m) vs %.
2. Mantener el dashboard sincronizado solo si cambia algo del modelo (no debería).
3. "With more time" (ya en el README): calibración con experimentos, modelo geo, ROI sobre margen, elasticidad propia vs cruzada, optimizador.

## Sesión 5 (6-oct-2026): iteración del deck con comentarios

- Branch vigente pasa a ser `claude/upbeat-davinci-5zpud1` (fast-forward de `friendly-goldberg`). Fuentes del deck en `deck/project/` sincronizadas con el artifact tras cada cambio.
- Cambios: slide 2 (dos cards de precio de góndola propio +40% vs competencia +8%; "Stores that sell us"; takeaway sin AI talk), slide 3 (barras "Relative price" con ours/theirs por año; "Other*" = clima, promos marca C, calendario, no explicado), slide 4 (rehecha: elasticidad −0.5 como big number; recomendación "Hold the price gap, test before further increases"), slides 4–6 sin "So what" (recomendaciones directas), plan alineado.
- Usuario: nada de "So what" explícito ni jerga; inglés básico; flechas solo para paso del tiempo.
- **Elasticidad por nivel de premium** (`src/12_elasticity_by_premium.py` → `elasticity_by_premium.csv`, espejo lineal GLS-AR en log volumen): única −0.49 (= Meridian). Piecewise con corte 1.7/1.8/1.9×: diferencia alto−bajo −0.14/+0.10/+0.23, ninguna significativa; cuadrática: puntos más planos con premium alto (−0.69 a 1.4× → −0.22 a 1.95×) pero intervalos enormes arriba de 1.8×. Conclusión: la data no muestra que la sensibilidad crezca con el premium, pero arriba de ~1.9× está mal medida (pocas semanas, confundido con 2024). Respalda "testear antes de subir", no "no hay lugar".
- Abierto: título de portada (6 opciones en el hilo), separar "Other" en barras o dejar nota.

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

- **Repo:** `fedemendez1/nimblegravity`, carpeta `fmcg_mmm/`. **Branch vigente: `claude/friendly-goldberg-x46lp2`** (sesión 4; contiene todo lo de `claude/great-rubin-edkjpe`). Leer siempre la más reciente.
- **Deck:** https://claude.ai/artifact/Hr4ZPiahCBL7Jx26wFT2HG · copia de fuentes en `deck/project/` (deck.json + `slides/*.html`).
- **Dashboard (artifact privado):** https://claude.ai/artifact/5Z4CppFB4yJPW2MYC4xThA. Se regenera con `python src/05_dashboard.py` y se republica desde `outputs/dashboard.html`.
- **Entorno:** `.venv` en la raíz del repo (no versionado). `pip install -r fmcg_mmm/requirements.txt` (Meridian 2.1.0). Correr los scripts desde `fmcg_mmm/src`.
- **Modelos:** `*.pkl` está en `.gitignore`, pero **se versionan con `git add -f`** (~3 MB c/u) para no volver a correrlos. En el repo:
  - Final: `mmm_stage1`, `mmm_final` (+ `_ho13`, `_ho26` de ambos).
  - Sensibilidad del final: `mmm_final_wide/tight/high` (priors), `mmm_final_h18/h22/h24` (umbral de calor).
  - Históricos: `mmm_f1` (base, screen), `mmm_cC`, `mmm_cCA`, `mmm_cCar` (+ `_ho13`).
  - El base viejo con cadenas completas (`mmm_base`) NO está guardado (no hace falta).
  - Args de `03_model.py`: `--draws screen` (~4 min), `--extra col1 col2` (drivers extra), `--ar-from mmm_x` (corrección AR), `--heat N` (umbral de heat_excess), `--holdout N`.
  - **Regla:** cada modelo nuevo que corramos → `git add -f` del `.pkl` + tablas, commit y push en el momento.

### Pipeline

| Script | Qué hace |
|---|---|
| `01_clean.py` | Checks y dataset limpio `data/clean/model_data.csv` |
| `02_eda.py` | Gráficos `eda_*` y tablas `eda_*` |
| `03_model.py` | Fit de Meridian. Args: `--tag --holdout --knots --roi-prior mu,sigma --draws quick/screen/full --no-season` |
| `04_diagnostics.py --model mmm_final` | Convergencia, fit (+ holdout 26/13), tests de residuos (Ljung-Box, Jarque-Bera, Breusch-Pagan), VIF, signos, elasticidad, ROI, contribuciones y curvas → `outputs/tables`, `outputs/figures` |
| `05_dashboard.py` + `dashboard_template.html` | Dashboard HTML |
| `06_sensitivity.py` | Compara variantes → `sens_*.csv` y su figura |
| `08_ar_twin.py [--no-ar]` | Gemelo bayesiano (PyMC) de la spec de Meridian con errores AR(1) estimados en conjunto. ~15 s → `tables/ar_twin.csv` |
| `09_variable_search.py` | Búsqueda de variables con modelo espejo lineal (replica residuos de Meridian, corr 0.99) → `tables/search_candidates.csv` |
| `07_residuals.py --model mmm_x` | Residuos semanales, ACF/DW por año, screening de todas las columnas crudas y lags vs. residuos, figura `diag_02_residual_autocorr_<model>` |
| `08_ar_twin.py --student-t --het` | Variantes del gemelo: colas pesadas y varianza que crece con el nivel → `ar_twin.csv` |
| `10_unconstrained.py` | MMM clásico sin restricciones (OLS + AR(1) GLS, signos libres, adstock/Hill por grid BIC), ROI por canal y por campaña → `unconstrained_roi.csv`, `unconstrained_shapes.csv` (~30 s) |
| `11_story.py` | Números del deck desde `mmm_final`: ROI por campaña, año contra año, efectos por unidad, ajuste semanal → `story_*.csv` |

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

## [HISTÓRICO, superado] Resultados del primer modelo (`mmm_base`, sesión 1)

> Superado por el modelo final (ver Estado actual). En particular, lo de "todos saturados" resultó ser efecto del prior.

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
- ~~**Curvas de respuesta:** todos los canales están saturados, con ROI marginal ~0.4–0.8.~~ (sesión 4b: no identificado, es el prior)

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

### Autocorrelación – en curso (resuelta en sesión 3, ver abajo)
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

### Próximos pasos de modelado (todo hecho en sesión 4)
1. ~~Revisar final full + holdout + sensibilidad de priors final.~~ Hecho. Luego `04_diagnostics.py --model mmm_final`, `05_dashboard.py` (apuntar a `mmm_final`), README (sacar AR de "With more time", agregar promo C y AR a la definición).
2. Decisiones abiertas: ROI por canal vs. total (¿agrupar en 2–3?), mostrar o no curvas de respuesta, base de contribuciones, sensibilidad umbral 20°C de heat_excess.
3. Knots 2/3: ya no hace falta.

### [HISTÓRICO] Próximo paso acordado en sesión 2 (superado)
1. `03_model.py --tag cC_k2 --knots 2 --draws screen --extra comp_c_promo_share` y lo mismo con `--knots 3`. Mirar ACF/DW, elasticidad de precio (¿sobrevive?) y holdout.
2. Si el precio sobrevive y baja la ACF → ese es el modelo final; si no → final = base + promo C, y presentar la autocorrelación como diagnóstico trabajado (DW ~1.3, intervalos ×1.4).
3. Dummies de período (inicio 2022, H1 2024) solo si hay razón de negocio.
4. Después: correr el final con cadenas completas + holdout 13, `04_diagnostics.py`, `07_residuals.py`, `05_dashboard.py`, actualizar README. Recién ahí storytelling.
5. Otras decisiones abiertas (de la sesión 2, aún sin discutir): ROI por canal vs. solo total (canales no identificados; ¿agrupar en 2–3?); mostrar o no curvas de respuesta (dependen del prior); promo propia ≈0; base de contribuciones (vs. mínimo observado → base 71%; agrupar estilo Nielsen base = fondo + precio + distribución + clima); umbral 20°C de heat_excess sin sensibilidad.

## Sesión 4 de modelado (5-oct-2026): VIF, colas, heterocedasticidad, umbral de calor

Modelado **cerrado**. `04_diagnostics.py`, dashboard y README ya apuntan a `mmm_final` (holdout 26 sem en tile).

- **Tests nuevos en `04_diagnostics.py`** → `diag_residual_tests.csv`, `diag_vif.csv` (también en el dashboard, panel Model health).
  - Ljung-Box p 0.76 / 0.67 / 0.80 / 0.32 (lags 1/4/13/26): autocorrelación resuelta.
  - VIF máx 8.7 (`temp_avg`, por `season_cos` −0.81 y `heat_excess` 0.74). Precio 3.4, distribución 3.2, media 1.5–3.1. Canales no identificados por señal chica, **no** por colinealidad.
  - Jarque-Bera p<0.001 (curtosis exceso 2.0; 3 semanas de calor ±13%: jul-22, ago-22, jun-23). Breusch-Pagan p<0.001 en kg, 0.02 en %.
- **Colas + heterocedasticidad: misma causa** (el ruido crece con el nivel de ventas; verano más ruidoso en kg). Gemelo PyMC (`08_ar_twin.py --het`, σ_t = σ·exp(δ·μ_t), δ 0.55): residuos estandarizados normales y homocedásticos (JB p 0.15, BP p 0.20, curtosis 0.66). Student-t sola NO lo arregla (ν 2.8, curtosis 4.7); t + het: ν 7.7, JB p 0.05.
  - Conclusiones: elasticidad −0.41 (−0.53/−0.30) con het vs −0.47; t −0.49; t+het −0.45. ROI total 1.06–1.15. → robusto; caveat de apéndice, no se cambia el modelo (Meridian no soporta ni t ni varianza variable).
  - Con het ρ sube a 0.58 e innovaciones ACF −0.16 (leve sobrecorrección).
- **Umbral de `heat_excess`** (`--heat 18/22/24`, screen, AR lag de `mmm_stage1` con 20°C) → `sens_heat.csv`: R² 0.924/0.930/0.932/0.927 (18/20/22/24), elasticidad −0.46/−0.47/−0.47/−0.50, ROI total 1.15/1.06/1.03/1.11. TV último y OOH primero en todos. 24°C normaliza residuos (JB p 0.44) pero empeora MAPE (4.5%) y ACF (0.08). **Se queda 20°C.**
- Veredicto: estadísticamente sólido para drivers, elasticidad y ROI total; no para rankear canales (limitación de data).

## Sesión 4b: storytelling, base vs incremental y defensa de los ROIs

- **Saturación y carryover NO identificados:** posterior de `ec_m` y `alpha_m` = prior en los 6 canales (ancho post/prior 0.97–1.03). mROI ≈ 0.45×ROI es mecánico (Hill slope 1, ec≈1). Causa: TV 3 semanas a GRPs constantes, partnership 13 sem constantes, OOH 5 sem; solo DV (5×) y search (2.8×) varían. → no vender "todo saturado"; curvas de respuesta al apéndice como supuesto.
- **Due-to año contra año** (referencia-libre; ahora en `src/11_story.py` → `story_due_to.csv`): 2022→23 −8.2% = precio −7.7, competencia −1.8, clima −1.4, media +0.5; 2023→24 +8.2% = distribución +13.3, precio −4.3, clima −1.6, competencia +1.5, media +0.7. Sin explicar ±1.1%.
- **Techo de detectabilidad** (ruido semanal 5.3%): TV ROI 1 ⇒ +6.3%/sem en 3 sem (~2σ por punto de ROI, ~1.4σ con carryover) → ROI ≥2 se vería. Social/partnership/OOH solo ROI ≥3.
- **Modelo sin restricciones estilo Nielsen** (`10_unconstrained.py`, OLS + AR(1) por GLS iterado, adstock/Hill por grid BIC, signos libres) → `unconstrained_roi.csv`, `unconstrained_shapes.csv`:
  - Por canal se cancelan entre canales que salieron juntos: TV −2.2 vs partnership +3.7 (abr-23), DV −1.2 vs OOH +9.5 (lanzaron la misma semana, abr-24). Search −236 = artefacto (£29k; absorbe la caída H1-2024).
  - Por campaña: TV+partnership 2023 **0.84** (−0.7/2.3); DV+OOH 2024 **1.13** (−0.8/3.0); social 2.2 (−1.3/5.7); **total sin search 1.26 (0.13–2.40), t 1.8**.
  - Lectura: el ~1 del modelo final NO lo pone el prior; la data sola da ~1.2. La partición entre canales simultáneos no está identificada; la lectura defendible es por campaña/flight.
  - Elasticidad sin restricciones −0.46 (igual al final).
  - Respuesta al "ROI 1 a 1 es un fiasco": el insight no es el número sino que la marca depende poco de la media; las palancas son precio y distribución; el plan de medios no permite medir canales (proponer escalonar lanzamientos y variar presión). El usuario lo aceptó.

## Sesión 4c: storytelling, MVP del deck (5-oct-2026)

**Modelo CERRADO** (decisión del usuario). Ahora solo storytelling.

### Guidelines del usuario para la presentación (respetar siempre)
- Director frente al cliente. Historia clara de cómo evolucionó la marca; **nada de ramas técnicas** en el cuerpo.
- **Menos es más:** poco texto, bullets claros, muy visual (waterfalls, big numbers). Yo tiendo a escribir de más: recortar.
- **So what** en cada slide: el cliente quiere un plan de marca accionable para maximizar revenue en TODO lo que controla (precio, promo, distribución, ejecución, media), con sentido común (nada de "subí distribución al 100%").
- Apéndice: slide de **model performance muy visual** (real vs modelo + residuos), da confianza sin entrar en detalle.
- Historia acordada: **producto poco dependiente de media**; palancas reales = precio y distribución.

### Deck (artifact Slides, privado)
- **URL:** https://claude.ai/artifact/Hr4ZPiahCBL7Jx26wFT2HG (título "Bottled Water Brand Review"). Copia de las fuentes en `fmcg_mmm/deck/project/` (deck.json + slides/*.html). Para editar en otra sesión: leer el artifact (`read` con `path` del slide) antes de republicar; republicar con `url` + `root` apuntando a una carpeta con `project/...`.
- En inglés (cliente UK). Fuentes: Domine (títulos) + DM Sans. Paleta: navy #14213D, fondo #F6F7F4, azul #2A6FDB, naranja negativo #D9662B/#B4501C.
- Slides: 1 cover · 2 growth (revenue +32% con volumen plano; precio/kg £0.76→£1.00, premium 1.5×→1.9×, distribución 39→43%) · 3 dueto (waterfall 2022→23→24: precio −12.8 / −6.6m kg, distribución −0.4 / +20.4, media+promo +1.8 / +1.6, otros −2.3 / −2.7) · 4 price (+10% premium → −4.5% vol → +5% revenue; ya estamos a 1.9× → subas en pasos chicos) · 5 volume (+3.6% vol por punto de distribución; +4% por °C sobre 20°C) · 6 promomedia (1 de cada 3 kg en promo, sin lift medible; media £1.06 por £1, 2024 £1.10 vs 2023 £0.86) · 7 plan (5 moves: precio, promo, distribución, verano, media) · Apéndice: 8 fit (real vs modelo + residuos, 93% / ±4% / 4.6% holdout) · 9 mediaroi (ROI por campaña con rangos 90%).
- Speaker notes con los caveats en cada slide.
- Notas de edición: formato de slide = HTML con estilos inline sobre lienzo 1920×1080 (sin CSS externo). Waterfall y rangos de ROI armados con divs posicionados (coordenadas calculadas desde `story_due_to.csv` / `story_campaign_roi.csv`, eje 140–172m kg). Gráfico de ajuste = SVG inline generado desde `story_fit.csv`. Si cambian números, recalcular posiciones.
- No se verificó el render visual del MVP (puede haber detalles de layout para corregir en la primera vuelta).

### Números (todos desde `mmm_final`, script `src/11_story.py`)
- `story_campaign_roi.csv`: TV+partnership 0.86 (0.42–1.58), video+OOH 1.10 (0.49–2.19), social 0.98 (0.32–2.60), total 1.06 (0.65–1.66).
- `story_due_to.csv`: año contra año por driver (con 90%). `story_effects.csv`: efectos por unidad + hechos por año. `story_fit.csv`: real vs modelo semanal.

### Abierto para iterar (ver también Estado actual → Próximos pasos)
1. Revisión del usuario slide por slide (espera iterar mucho).
2. ¿Dimensionar el plan? (p. ej. cuánto revenue vale +1 pt de distribución o −X pp de promo). Hoy solo dirección; no inventar números sin modelo.
3. Promo: el insight "sin lift medible" se apoya en `promo_intensity` ≈ 0 (90%: −2% a +5% por +10pp). Confirmar con el usuario cómo lo quiere decir (no hay márgenes).
4. Waterfall: eje truncado en 140m kg (anotado en el footer); decidir si se prefiere en %.
5. Republicar dashboard solo si cambia algo del modelo (no debería).

## Gotchas técnicos

- **Priors en Meridian 2.1:** se exigen en float64. Usar `backend.tfd.LogNormal(np.float64(...))`.
- **`save_meridian` (serde)** requiere `google-meridian[schema]`. Usamos `model.save_mmm` (pickle, deprecado pero funciona).
- **Pip:** pip del sistema falla (paquetes de Debian). Usar el venv.
- **Background:** `pkill -f 03_model.py` mata el propio shell si el patrón está en el comando. Evitarlo.
- **Gráficos:** paleta de `style.py`, una sola escala por eje.
- **Meridian `incremental_outcome`:** devuelve canales de media primero y después los non-media treatments; cortar `[..., :n_media]`.
- **Scripts que importan `04_diagnostics`:** correr desde `src/` (o `PYTHONPATH=.`).
- **Contenedor efímero:** el `.venv` no persiste entre sesiones; reinstalar con `pip install -r fmcg_mmm/requirements.txt` (~3 min). Los .pkl sí están en git.
