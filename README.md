# 🛒 Retail & E-Commerce Sales ETL Pipeline  
### Arquitectura Medallion en Azure Databricks

![Databricks](https://img.shields.io/badge/Databricks-ETL-orange?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Transformations-blue?style=for-the-badge&logo=apache-spark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-green?style=for-the-badge)
![ADLS](https://img.shields.io/badge/Azure%20Data%20Lake-Storage-blue?style=for-the-badge&logo=microsoft-azure)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-black?style=for-the-badge&logo=githubactions)

Pipeline automatizado de datos para analizar ventas de **e-commerce** y **tiendas físicas** utilizando arquitectura **Medallion (Bronze → Silver → Gold)** en Azure Databricks, con despliegue continuo mediante GitHub Actions y almacenamiento en Azure Data Lake Storage Gen2.

---

## 📌 Descripción

Proyecto final del curso **Ingeniería de datos e IA con Databricks – SmartData**.

Se construye un pipeline ETL “enterprise-like” que:

- Integra datos de **dos canales de venta**:
  - Ventas online (e-commerce).
  - Ventas de tiendas físicas (superstore / supermercado).
- Implementa la arquitectura **Medallion** sobre **Delta Lake**.
- Se conecta a la capa Raw **únicamente con Managed Identity**.
- Evita usar DBFS o Volúmenes como capa Raw (solo ADLS Gen2).
- Despliega automáticamente el código desde el entorno de **Desarrollo** hacia **Producción** usando **GitHub Actions** y **Databricks Repos**.
- Orquesta el ETL en Producción con **Databricks Workflows**.

### ✨ Características principales

- 🚀 **ETL automatizado** – pipeline completo Bronze → Silver → Gold en PySpark.  
- 🧱 **Arquitectura Medallion** – separación clara de capas y responsabilidades.  
- ⭐ **Modelo casi dimensional** – fact/dims para análisis de negocio por canal.  
- 🤖 **CI/CD integrado** – despliegue del código a Databricks-PROD vía GitHub Actions.  
- 🛡️ **Seguridad** – acceso a Data Lake con **Managed Identity** y GRANTs por capas.  
- 📊 **Visualización** – dashboards sobre las tablas Gold (Power BI o Databricks Dashboards).

---

## 🧮 Datasets utilizados

> Los CSV se descargan desde Kaggle y se cargan en el contenedor `raw` del Data Lake.

### 1. Ventas E-Commerce (canal online)

- Dataset: **E-commerce Sales Prediction** (Kaggle).  
- Información de pedidos, clientes, productos, fechas, montos, descuentos, etc.
- Ubicación en el Data Lake:

```text
raw/ecommerce/*.csv
```

### 2. Ventas en tienda (canal físico)

- Dataset: **Supermarket / Superstore dataset bundle** (Kaggle).  
- Ventas de tiendas físicas, categorías de producto, segmentación, profit, etc.
- Ubicación en el Data Lake:

```text
raw/superstore/*.csv
```