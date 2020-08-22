
import click

from spreadsheets_fetcher import fetcher


@click.command()
@click.argument('spreadsheets_ids', nargs=-1)
def fetch_spreadsheets_and_echo(spreadsheets_ids):
    """
    Download each id of SPREADSHEET_IDS

    SPREADSHEET_IDS a list of spreadsheet ids shared using "anyone on the internet with this link can view"
    """
    values = [fetcher.fetch_spreadsheet(spreadsheet_id) for spreadsheet_id in spreadsheets_ids]
    click.echo(values)


if __name__ == '__main__':
    fetch_spreadsheets_and_echo()