import click
import pandas as pd
from functools import partial


def city_field_column(city_name, field):
    return f"{city_name}.{field}"


@click.command()
@click.option(
    "--input-path",
    default="output/city_stats.csv",
    help="The path of the city stats csv file",
)
@click.option(
    "--output-path",
    default="output/metrics.csv",
    help="The path of the metrics result csv file",
)
@click.option(
    "--city-name",
    default="Mumbai",
    help="The name of the city to extract data from the input file",
)
def calculate_city_stats_metrics(input_path, output_path, city_name):
    """
    Accepts an INPUT_PATH to which makes calculations about the states of the CITY_NAME, writes the calculations metrics to the OUTPUT_PATH file.

    INPUT_PATH A string with path of the city states file.
    OUTPUT_PATH A string with the path to which the csv will be written.
    CITY_NAME A string with the name of the city in the INPUT_PATH columns.
    """
    click.echo(f"Calculating metrics of city {city_name} from file {input_path} ")
    df = pd.read_csv(input_path)
    df.index = df["Unnamed: 0"]
    df.index = pd.to_datetime(df.index)
    df.drop("Unnamed: 0", axis=1, inplace=True)
    field_column = partial(city_field_column, city_name)

    df[field_column("total.deceased.shift")] = df[field_column("total.deceased")].shift(
        1
    )
    df[field_column("Levitt.Metric")] = (
        df[field_column("total.deceased")] / df[field_column("total.deceased.shift")]
    )
    df[field_column("MA.daily.tests")] = (
        df[field_column("delta.tested")].rolling(window=21).mean()
    )
    df[field_column("MA.daily.positivity")] = (
        df[field_column("delta.confirmed")] / df[field_column("delta.tested")]
    )
    df[field_column("percent.case.growth")] = df[
        field_column("delta.confirmed")
    ].pct_change()
    click.echo(
        f"Wrote metrics of {city_name} for {df.shape} records to file {output_path}."
    )
    df.to_csv(output_path)


if __name__ == "__main__":
    # execute only if run as a script
    calculate_city_stats_metrics()
