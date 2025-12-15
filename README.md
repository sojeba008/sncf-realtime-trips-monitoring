# 🚄 Infocentre Temps Réel SNCF

> 💡 **Solution d'ingénierie de données temps réel pour la surveillance et l'analyse de la ponctualité et des alertes de service des trains SNCF (TGV, Intercités, TER).**

## 🌟 Vue d'Ensemble

Ce projet met en place un **Infocentre SNCF en temps réel**, permettant de suivre et d’analyser la circulation ferroviaire sur l’ensemble du réseau. Les données brutes des flux GTFS-RT et SIRI sont transformées et enrichies pour produire des indicateurs clés et des statistiques consolidées.

L’infrastructure génère automatiquement des **KPI temps réel** tels que :

- Nombre de trains actifs actuellement ou sur la dernière heure.
- Nombre de trains en retard et variation par rapport à la journée précédente.
- Retard moyen journalier et comparaison avec la moyenne historique.
- Nombre de gares actives et évolution horaire.
- Taux de retard par jour, par type de train et par région.
- Retards générés et rattrapés par région, ainsi que leur corrélation avec l’heure de passage.
- .... etc

Les **analyses station** permettent de suivre :

- Le trafic quotidien (arrivées/départs) et les trains en retard.
- Le taux de ponctualité et les retards moyens par gare.
- L’utilisation des quais et leur trafic total.

Toutes ces mesures sont accessibles via des vues et *materialized views* PostgreSQL, ce qui permet une **consultation rapide et dynamique** des indicateurs pour le reporting ou la visualisation en BI.

---

## 🚀 Architecture Technique

Le projet repose sur la pile technologique suivante, entièrement conteneurisée avec **Docker** :

| Couche | Outil | Rôle |
| :--- | :--- | :--- |
| **Orchestration** | **Apache Airflow** | Planification et exécution des workflows de collecte et de transformation (**DAGs**) des données temps réel et des référentiels. |
| **Stockage** | **PostgreSQL (PostGIS)** | Base de données relationnelle servant de **Data Warehouse** (`sncf_trips`) et de base de métadonnées pour Airflow (`airflow`). Stocke les faits temps réel et les tables de dimensions (Gares, Trajets, Géographie). |
| **Visualisation (Actuel)** | **Pentaho Server** | Plateforme de Business Intelligence utilisée pour générer des rapports et des tableaux de bord. |
| **Visualisation (Cible)** | **Apache Superset** | Nouvelle plateforme de BI pour des tableaux de bord modernes et interactifs. |
| **Conteneurisation** | **Docker / Docker Compose** | Configuration et déploiement de l'environnement de développement et de production. |


---

## 🔗 Sources de Données

Les flux de données sont extraits des API Open Data de la SNCF et des référentiels géographiques nationaux.

| Catégorie | Source | Endpoint / Référence |
| :--- | :--- | :--- |
| **Temps Réel (Facts)** | GTFS-RT Trip Updates & Service Alerts (TU/SA) | `https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates` |
| **Temps Réel (Alternative)** | SIRI Estimated Timetable (ET Lite) | `https://proxy.transport.data.gouv.fr/resource/sncf-siri-lite-estimated-timetable` |
| **Référentiel Trajets**| GTFS Théorique / NeTEx | `https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip` |
| **Référentiel Gares** | Liste des gares du Réseau Ferré National | `https://ressources.data.sncf.com/explore/dataset/liste-des-gares/information/` |
| **Référentiel Géographique** | Données INSEE (Régions, Dép., Villes) | `https://www.data.gouv.fr/datasets/regions-departements-villes-et-villages-de-france-et-doutre-mer/` |

---

## 🏗️ Structure du Projet


---

## ⚙️ Démarrage Rapide

Ce projet utilise Docker Compose pour orchestrer l'ensemble des services (Airflow, PostgreSQL, Pentaho, etc.).

### 1. Prérequis

* **Docker**
* **Docker Compose**

### 2. Démarrage de l'environnement

1.  Cloner le dépôt :
    ```bash
    git clone #####
    cd #####
    ```
2. Configuration de l'environnement

Créez un fichier `.env` (à partir du fichier template) à la racine du projet pour définir les variables d'environnement nécessaires au fonctionnement des services.

**Exemple des variables d'environnement critiques (fichier `.env`) :**

```env
# Configuration Airflow (Métadonnées)
AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
AIRFLOW_DB=airflow
AIRFLOW_HOST=postgres

# Configuration PostgreSQL (Utilisateur/Base par défaut pour l'initialisation)
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres

# Configuration Data Warehouse (DWH - Base de données des données SNCF)
DWH_HOST=postgres
DWH_USER=etl_user
DWH_PASSWORD=etl_password
DWH_DB=sncf_trips
DWH_PORT=5432

# Utilisateur Admin Airflow
AIRFLOW_ADMIN_USERNAME=airflow
AIRFLOW_ADMIN_PASSWORD=airflow
```

2.  Lancer la commande de votre script `run.sh` pour l'initialisation et le démarrage :
    ```bash
    ./run.sh
    # ou si vous utilisez docker-compose directement :
    # docker-compose up -d --build
    ```
3.  Attendre quelques minutes que tous les conteneurs soient opérationnels (vérifiez avec `docker ps`).

### 3. Accès aux Interfaces

| Service | URL | Identifiants par Défaut | Remarques |
| :--- | :--- | :--- | :--- |
| **Airflow UI** | `http://localhost:[8084 - Port Airflow UI]` | **User :** `[airflow_user]` / **Pass :** `[airflow_pass]` | Interface web pour gérer et monitorer les pipelines ETL et les tâches en temps réel. |
| **PostgreSQL** | `[http://localhost:[8082 - Port Postgresql]/database]` | **User :** `[postgres_user]` / **Pass :** `[postgres_pass]` | Base de données relationnelle et spatiale (PostGIS) pour stocker les données du Data Warehouse. |
| **Pentaho Server (Actuel)** | `http://localhost:[8086 - Port Pentaho]` | Aucun identifiant de connexion n'est necessaire. | Serveur BI pour création et publication de rapports et tableaux de bord. |
| **Superset (Futur)** |  | **User :** `[superset_user]` / **Pass :** `[superset_pass]` | Remplacera Pentaho pour la visualisation interactive et l’exploration des données. |


---



## 🤝 Contribution

TODO