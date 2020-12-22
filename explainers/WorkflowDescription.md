
## Dashboard Dependencies and Pipeline Description (Draft)

|               Graph               |  Sheets used  |        Columns used       | Documented | Code                                      |Source|
|:---------------------------------:|:-------------:|:-------------------------:|:----------:|:-----------------------------------------:|:---|
|           City dropdown           |    metrics    |          district         |     n/a    |pipeline/pipeline/extract_history_file.py|covid19india API|
|            Date slicer            |    metrics    |            date           |     n/a    |pipeline/pipeline/extract_history_file.py|covid19india API|
|           Summary stats           |    metrics    |      delta.confirmed      |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|           Summary stats           |    metrics    |       delta.deceased      |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|           Summary stats           |    metrics    |        delta.tested       |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|           Summary stats           |    metrics    |      delta.recovered      |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|           Summary stats           |    metrics    |            date           |     n/a    |pipeline/pipeline/extract_history_file.py|covid19india API|
|           Summary stats           |    metrics    |     delta.hospitalized    |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
| Case growth rate and active cases |    metrics    | delta.percent.case.growth |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
| Case growth rate and active cases |    metrics    |       spline.active       |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|             Fatalities            |    metrics    |       delta.deceased      |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|             Fatalities            |    metrics    |      spline.deceased      |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|             Fatalities            |    metrics    |            date           |     n/a    |pipeline/pipeline/extract_history_file.py|covid19india API|
|               Tests               |    metrics    |        delta.tested       |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|               Tests               |    metrics    |            date           |     n/a    |pipeline/pipeline/extract_history_file.py|covid19india API|
|               Tests               |    metrics    |     MA.21.daily.tests     |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|          Hospitalizations         |    metrics    |            date           |     n/a    |pipeline/pipeline/extract_history_file.py|covid19india API|
|          Hospitalizations         |    metrics    |     delta.hospitalized    |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|          Hospitalizations         |    metrics    |    spline.hospitalized    |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|             Recoveries            |    metrics    |            date           |     n/a    |pipeline/pipeline/extract_history_file.py|covid19india API|
|             Recoveries            |    metrics    |      delta.recovered      |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|             Recoveries            |    metrics    |      spline.recovered     |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|        Test positivity rate       |    metrics    |            date           |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|        Test positivity rate       |    metrics    |   MA.21.delta.positivity  |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|        Test positivity rate       |    metrics    |      delta.positivity     |     n/a    |pipeline/pipeline/calculate_metrics_file.py|covid19india API|
|         Reproduction rate         |       Rt      |            date           |     n/a    |                                           ||
|         Reproduction rate         |       Rt      |         mean.mean         |     n/a    |                                           ||
|         Reproduction rate         |       Rt      |       CI_lower.mean       |     n/a    |                                           ||
|         Reproduction rate         |       Rt      |       CI_upper.mean       |     n/a    |                                           ||
|           Doubling time           | doubling_time |       doubling.time       |     n/a    |                                           ||
|           Doubling time           |    metrics    |      delta.confirmed      |     n/a    |pipeline/pipeline/calculate_metrics_file.py||
|           Doubling time           |    metrics    |            date           |     n/a    |Probably calculate_metrics_file.py                                           ||
|           Levitt metric           |    metrics    |            date           |     n/a    |Probably calculate_metrics_file.py                                           ||
|           Levitt metric           |    metrics    |       levitt.Metric       |     n/a    |pipeline/pipeline/calculate_metrics_file.py||

### Ward data 
**source:** gsheet <br />
**destination:** gsheet <br />
[ward_metrics](https://github.com/swb-ief/etl-pipeline/blob/master/pipeline/pipeline/ward_metrics.py) <br />
call's -> [ward_data_computation](https://github.com/swb-ief/etl-pipeline/blob/master/pipeline/pipeline/ward_data_computation.py)

## code call structure
```
.github/workflows/pipeline-schedule.yml (with call: python -m luigi --module pipeline.tasks.spreadsheets ...)
.github/workflows/pipeline.yml (with call: python -m luigi --module pipeline.tasks.spreadsheets ...)
  -> pipeline/pipeline/tasks/spreadsheets.py
    -> pipeline/pipeline/extract_history_file.py
      -> pipeline/pipeline/calculate_metrics_file.py
```

## Dependencies 

Garima clarified that most of these sheets and columns are generated from running calculate_metrics.py


**1) calculate_metrics.py**
- Currently, the primary function, calculate_metrics(), outputs csv's to output/percentages_for_hospitalizations.csv and output/city_metrics.py
- In some of the existing docs, I am reading that calculate_metrics.py is executed as part of the CalculateCityMetricsTask; however, I don't see that task or its wrapper task, SWBPipelineWrapper (referring to the etl-pipeline/pipeline README) in any of the existing github action workflows. 
- In the data_pipeline task, `pipeline.tasks.spreadsheets AllDataGSheetTask` is run. [AllDataGSheetTask](https://github.com/swb-ief/etl-pipeline/blob/6e1096d0b170103504e68df71e4c849f2abe3188/pipeline/pipeline/tasks/spreadsheets.py#L32) is where the csv outputs of calculate_metrics.py (city_metrics.csv and percentages_for_hospitalizations.csv) are piped to Google Sheets using **new** names. 
- For example, city_metrics.csv is piped to the "metrics" Google Sheet. 




