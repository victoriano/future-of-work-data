import typer
from rich import print as rprint
from rich.markdown import Markdown
from rich.panel import Panel
from rich.pretty import pprint, Pretty

from . import db as dbx
from .etl import esco, onet

app = typer.Typer()


@app.command("dbs")
def list_dbs():
    """List all registered DuckDB databases."""
    pprint(list(dbx.DBS.keys()))


@app.command("tables")
def list_tables(db: str = typer.Option()):
    """List all tables in specified DuckDB database."""
    tables = dbx.list_tables(db)
    panel = Panel(Pretty(tables), title=f"DB: {db}", expand=False)
    rprint(panel)


@app.command("doc")
def make_doc(db: str = typer.Option()):
    """Create markdown documentation for a DuckDB database."""
    path = dbx.DBS[db]
    name = path.stem
    output_path = path.parent / f"{name}.md"
    md = dbx.generate_markdown(str(path), original_db_name=db)
    with open(output_path, "w") as f:
        f.write(md)

    rprint(Markdown(md))
    rprint(f"\nDocumentation written to: [bold]{output_path}[/bold]")


@app.command("etl")
def etl(db: str = typer.Option("")):
    if db == "esco":
        esco.to_duckdb()
    elif db == "onet":
        onet.to_duckdb()
