# NYC Restauracje — Business Intelligence

Projekt na kurs **Systemy Business Intelligence, PWr**. Repozytorium zawiera gotową,
połączoną infrastrukturę do samodzielnego zbudowania rozwiązania BI dla danych NYC.

Po uruchomieniu stos jest pusty:

- PostgreSQL ma bazę `nyc`, rozszerzenie PostGIS i pusty schemat `staging`,
- ClickHouse ma pustą bazę `nyc`,
- Superset ma gotowe połączenia `ClickHouse NYC` i `PostGIS staging`, ale nie ma
  datasetów, wykresów ani dashboardów,
- Dagster nie ma jeszcze assetów,
- Prefect nie ma jeszcze flow, deploymentów ani uruchomień.

## Uruchomienie

1. Uruchom Docker Desktop.
2. Otwórz terminal w folderze z `docker-compose.yml`.
3. Wpisz:

   ```text
   docker compose up -d --build --wait --wait-timeout 600
   ```

4. Otwórz **http://localhost**. Wspólny login do usług z logowaniem to
   **admin / admin**.

Pierwszy build wymaga internetu.

## Usługi

| Narzędzie | Adres lokalny | Rola |
|---|---|---|
| Portal | http://localhost | linki i status usług |
| Superset | http://localhost:8089 | analiza danych, datasety, wykresy i dashboardy |
| Pgweb | http://localhost:8081 | przeglądanie PostgreSQL; bez logowania i tylko do odczytu |
| Prefect | http://localhost:4200 | uruchamianie i obserwowanie flow |
| Dagster | http://localhost:3000 | definicje assetów i historia materializacji |
| ClickHouse | http://localhost:8123/play | edytor SQL, baza `nyc`; port native `9000` |
| PostgreSQL + PostGIS | `localhost:5433`, baza `nyc`, schemat `staging` | staging i obliczenia przestrzenne; np. DBeaver |

Login i hasło są zdefiniowane **raz** jako `BI_USER` i `BI_PASSWORD` w `.env`.
Pgweb, Prefect, Dagster i portal nie wymagają logowania. Porty są dostępne w sieci
lokalnej; jawne hasła i brak TLS są celowe w projekcie studenckim — nie wystawiać go
do internetu.

Jeśli port 80 jest zajęty, zmień `PORTAL_PORT=80` w `.env`, np. na `8090`, i otwórz
`http://localhost:8090`.

## Rozpoczęcie pracy

Flow Prefect dodaj w `pipeline/`, a jego deployment dopisz do listy `deployments` w
`pipeline/serve.py`. Assety Dagster dodaj w `dagster/definitions.py`.

## Zatrzymanie

```text
docker compose down
```

Ta komenda zachowuje stan usług. `docker compose down -v` usuwa wolumeny wyłącznie
projektu `nyc-bi`.

## Struktura

```text
portal/             strona startowa
pipeline/           kod flow Prefect i lista deploymentów
dagster/            definicje assetów i konfiguracja metadanych
docker/             obrazy, SQL i konfiguracja usług
```
