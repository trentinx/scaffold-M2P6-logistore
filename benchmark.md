# Benchmark Parquet+DuckDB  vs PostgreSQL
## Méthodologie

- Génération de fichiers movements et products au format csv aux paliers 10, 100K et 1M lignes
- Transformation des ces fichiers au format parquet
- Génération de tables PostgreSQL 


## Résultats
| Requête | Moteur|  Small | Medium | Large |
|-|-|-|-|-|
|Stock courant | PostgreSQL | 2.35 ms | 15.89 ms | 71.29 ms |
|| DuckDB/Parquet | 4.53 ms | 5.01 ms | 10.13 ms |
|Agrégation mensuelle | PostgreSQL | 3.18 ms | 24.95 ms | 271.97 ms |
|| DuckDB/Parquet | 1.60 ms | 5.55 ms | 45.43 ms |
|Alerte rupture (jointure) | PostgreSQL | 2.20 ms | 14.39 ms | 159.98 ms |
|| DuckDB/Parquet | 1.79 ms | 3.37 ms | 16.84 ms |
| Taille sur disque (mouvements) | PostgreSQL |1264 kB  | 12 MB  |  123 MB  |
| | Parquet| 0.65 MB  | 6.24 MB  | 59.56 MB  |

## Analyse

**Supériorité analytique** : le benchmark confirme que DuckDB est optimisé pour les agrégations avec la requête « agrégation mensuelle », où il  est jusqu'à 6 fois plus rapide que PostgreSQL sur le volume Large (45.43 ms contre 271.97 ms).

**Passage à l'échelle** : pour le `stock courant`, PostgreSQL est légèrement  performant sur de très petits volumes (2.35 ms contre 4.53 ms), ce qui reflète son efficacité sur des recherches simples par ligne. cependant, dès que le volume augmente (Medium et Large), DuckDB prend largement l'avantage, confirmant sa meilleure gestion des grands jeux de données grâce à son moteur vectorisé.

**Efficacité des jointures** : la requête `alerte rupture` (incluant une jointure) montre un écart massif sur le volume Large (16.84 ms pour DuckDB contre 159.98 ms pour PostgreSQL). cela valide l'argument de l'article sur la supériorité du stockage colonnaire pour scanner et filtrer de grandes tables.

**Optimisation du stockage** : Les mesures des espaces disque montrent que DuckDB (via Parquet) consomme environ deux fois moins d'espace que PostgreSQL (59.56 MB contre 123 MB pour le volume Large), prouvant l'efficacité de la compression liée à cette architecture.

## Explications

DuckDB (le moteur analytique) : il s'agit d'une base de données OLAP intégrée (sans serveur) qui utilise un stockage colonnaire et une exécution vectorisée. elle est conçue pour des calculs analytiques rapides sur de grands volumes de données, directement dans l'application, sans latence réseau.

PostgreSQL (le socle transactionnel) : c'est un système OLTP robuste, basé sur un modèle client-serveur et un stockage par lignes. il est idéal pour gérer de nombreux utilisateurs simultanés, des transactions complexes et garantir l'intégrité des données (ACID).

## Recommendations

Utiliser PostgreSQL comme source de vérité pour les opérations courantes et DuckDB pour accélérer les rapports, les tableaux de bord ou le traitement de fichiers volumineux.