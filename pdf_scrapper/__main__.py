import os
from urllib.request import urlretrieve

import click
import pandas as pd

from .stop_covid_dashboard_scrapper import scrap_pdf_to_csv


@click.command()
@click.option(
    "--dashboard-pdf-url",
    default="http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf",
    show_default=True,
)
@click.option("--output", default="stopcovid-dashboard.pdf", show_default=True)
def download_pdf(dashboard_pdf_url, output):
    datetime_suffix = pd.Timestamp.utcnow().isoformat()
    click.echo(f"Fetching {dashboard_pdf_url} into {output}")
    return urlretrieve(dashboard_pdf_url, output)


@click.command()
@click.argument("pdf-files", nargs=-1, required=True)
@click.argument("output-dir", nargs=1, required=True)
@click.option(
    "--page-with-positive-breakdown",
    default=22,
    help="The page containing the ward-wise breakdown of positive cases.",
    show_default=True,
)
def scrap_pdf(pdf_files, output_dir, page_with_positive_breakdown):
    """Given a PDF_FILE tries to fetch the data from all wards.

    We assume the PDF_FILE has been downloaded from http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf.

    PDF_FILES A list of paths to the pdf files.
    OUTPUT_DIR The path to the output directory.
    """
    if not os.path.isdir(output_dir):
        raise IOError(f"Output: {output_dir} is not a directory")

    paths = []
    for pdf_file in pdf_files:
        date, csv_path = scrap_pdf_to_csv(
            pdf_file, output_dir, page_with_positive_breakdown
        )
        paths.append(csv_path)
        click.echo(f"Data from {date} has been downloaded to {csv_path}")

    click.echo(f"Saved the following files {paths}")
    return paths


@click.group()
def cli():
    pass


cli.add_command(download_pdf)
cli.add_command(scrap_pdf)

if __name__ == "__main__":
    cli()
