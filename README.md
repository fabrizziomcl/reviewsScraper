# Google Maps Reviews Scraper (High-Performance Edition)

Un scraper avanzado basado en **Playwright (Async) y Multiprocessing** que extrae reseñas de Google Maps de manera masiva. Creado originalmente para extraer datos, ahora cuenta con una arquitectura de alto rendimiento que permite saturar tu conexión de red y CPU para extraer reseñas de miles de lugares concurrentemente sin ser detectado como bot.

## 🗺️ Project Scope

This repository (**googlemaps-reviews-scraper-es**) represents **Phase 2** of our Gastronomic Big Data Pipeline. Its primary responsibility is the high-performance, asynchronous extraction of user reviews from the geographic entities ingested previously.

> **Looking for the initial places ingestion?**
> For **Phase 1** (massive geographic orchestration and deduplication of restaurants), please see our complementary repository: [mapScraper](https://github.com/christivn/mapScraper).

---

## ⚡ Características Principales

1. **Arquitectura Multiproceso + Asyncio:** En lugar de usar hilos tradicionales, el orquestador (`orchestrator.py`) lanza múltiples procesos del sistema operativo. Cada proceso tiene su propio navegador Chromium e hilos asíncronos (`asyncio`), evadiendo el GIL de Python y maximizando el uso de la CPU.
2. **Bloqueo Extremo de Red (Stealth):** Para evitar consumir gigabytes de RAM y ancho de banda, el scraper intercepta y bloquea la descarga de mapas vectoriales, imágenes, videos y scripts de analíticas de Google, cargando solo el HTML necesario en milisegundos.
3. **Escritura Segura Fragmentada:** Los resultados se guardan en archivos CSV temporales por cada proceso (ej. `.p0`, `.p1`) que se fusionan al instante al terminar, eliminando cuellos de botella por bloqueos de escritura (`locks`).

---

## 📋 Requisitos

| Requirement | Versión |
|---|---|
| Python | ≥ 3.9 |
| SO | Windows, Linux o macOS |

*(No necesitas descargar ChromeDriver, Playwright gestiona sus propios navegadores).*

---

## 📦 Instalación

```bash
git clone https://github.com/christivn/googlemaps-reviews-scraper-es.git
cd googlemaps-reviews-scraper-es

# Crear entorno virtual con Conda (Python 3.11 recomendado)
conda create -n reviews-scraper python=3.11 -y

# Activar entorno
conda activate reviews-scraper

# Instalar dependencias
pip install -r requirements.txt

# Descargar los navegadores de Playwright (CRÍTICO)
playwright install chromium
```

---

## 🚀 Uso: Raspado Masivo (Orquestador)

Para extraer datos a gran escala (ej. 50,000 restaurantes), usa el orquestador paralelo.

```bash
# Prueba con 4 workers (Levantará 1 proceso Chromium)
python orchestrator.py --input data/input/places_peru.csv --workers 4 --max-reviews 15000

# Extracción Extrema con 12 workers (Levantará 3 procesos Chromium paralelos)
python orchestrator.py --input data/input/places_peru.csv --workers 12 --max-reviews 15000
```

### Parámetros del Orquestador:
*   `--input`: Ruta al archivo CSV con los lugares a extraer (Debe tener las columnas `id` y `url_place`).
*   `--workers`: Cantidad total de workers asíncronos. El sistema automáticamente dividirá estos workers en procesos de a 4.
*   `--max-reviews`: Límite de reseñas por lugar.
*   `--output-dir`: Carpeta donde se guardará el archivo `reviews_raw.csv` final.

---

## 🛠️ Herramientas de Desarrollo

### Benchmark de Rendimiento
Si quieres probar cuántos workers tolera tu computadora antes de iniciar un raspado de 10 horas, usa la herramienta de benchmark:

```bash
python utils/benchmark_workers.py --sample 50 --max-reviews 50 --configs 4,8,12
```
Esto creará pruebas aisladas y te mostrará una tabla de rendimiento comparativa (Throughput, Tiempo Promedio).
