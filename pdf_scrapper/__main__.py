import os

import click

from .stop_covid_dashboard_scrapper import scrap_pdf_to_csv


@click.command()
@click.argument("pdf_files", nargs=-1, required=True)
@click.argument("output_dir", nargs=1, required=True)
def scrap_pdf(pdf_files, output_dir):
    """Given a PDF_FILE tries to fetch the data from all wards.

    We assume the PDF_FILE has been downloaded from http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf.

    Args:
        pdf_files ([string or file handle]): The path to the pdf file or a buffer of the file.
        output_dir ([string]): The path to the output directory.
    """
    if not os.path.isdir(output_dir):
        raise IOError(f"Output: {output_dir} is not a directory")

    paths = []
    for pdf_file in pdf_files:
        date, csv_path = scrap_pdf_to_csv(pdf_file, output_dir)
        paths.append(csv_path)
        click.echo(f"Data from {date} has been downloaded to {csv_path}")

    click.echo(f"Saved the following files {paths}")
    return paths

if __name__ == "__main__":
    scrap_pdf()