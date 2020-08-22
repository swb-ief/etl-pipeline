# etl-pipeline


ETL Pipeline.


## Configuring the project

We use [pipenv](https://github.com/pypa/pipenv) to manage dependencies and setup a virtualenv. 

You can install following the [project's README instructions](https://github.com/pypa/pipenv#installation) 
or using [pipx](https://pypi.org/project/pipx/).

Once installed run the following to install all dependencies:

```shell script
pipenv install
```

If using PyCharm you can configure your project to use pipenv following this guide:

- https://www.jetbrains.com/help/pycharm/pipenv.html

To add new dependencies run the following:

```shell script
pipenv install pandas numpy requests
```

Each time you install new packages the `Pipfile.lock` gets updated, however if you want to have an up to 
date `requirements.txt` you can run the following command:

```shell script
pipenv run pip freeze > requirements.txt
```


The same can be used to run the project's scripts:

```shell script
pipenv run python -m spreadsheet_fetcher sp_id
```

## Fetching spreadsheets

We can fetch google spreadsheet that have been **previously shared to anyone with the link** using the 
following command:

```shell script
export SPREADSHEET_API_KEY=ApiKey 
pipenv run python -m spreadsheet_fetcher 1Gp5qI7vJnrXCwiO_3K54zRVBCVyMbFoOd86YvWcRHmE vT6RKqvY0VzMaN7pKyYPyVXvUYR5cu3L5Z0sTeayDRE72xCXqVU-rhgyAucjGMJDDG5rXRKInPChqrJ 
``` 

In order to be able to run this command we first need to obtain a valid API Key which can be done following this guide:

- https://cloud.google.com/docs/authentication/api-keys

This key **MUST** be restricted to only be used with the **Google Sheet API** and preferable it should also
be restricted to certain IPs.

### TODO

Currently the script just makes fetch the spreadsheet and echo the results, for an example on how to treat the 
response to do other things like _write a csv_ look at the 
[spreadsheets_fetcher/sheets_to_csv.py](spreadsheets_fetcher/sheets_to_csv.py) file.

In the future we plan to write the data from the csv to another data storage.   

## Github Actions

### Lint

Runs a [black](https://pypi.org/project/black/) check on `pull requests` and `pushes` to github.
This workflow will fail if the following command fails:

```shell script
black --check .
```

To fix errors with the litter you can run the following command locally:

```shell script
pipenv run black .
```

That command will update your files with the _black formatting_ so it's recommended that you 
don't have anything on stage.

### fetch_spreadsheets

Fetch the spreadsheets using the `spreadsheets_fetch` module.

TODO: This is still a work in progress, since we don't know what to do with the data.  