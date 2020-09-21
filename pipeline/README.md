# SWB covid pipeline

This pipeline is built using luigi to schedule and organize tasks.

## Workflows

We currently have to different workflows on for scrapping the mumbai wards pdf
and other to calculate metrics from the covid19india.org API.

### Running scrapping tasks

```sh
poetry run python -m luigi --module pipeline.tasks.stopcoronavirus_mcgm_scrapping ExtractDataFromPdfDashboardWrapper --local-scheduler
```

### Running tasks to calculate metrics from covid19india.org API

```sh
poetry run python -m luigi --module pipeline.tasks.cities_metrics CalculateMetricsTask --city-name 'Mumbai' --states-and-districts '{"MH": ["Mumbai"]}'  --local-scheduler
```