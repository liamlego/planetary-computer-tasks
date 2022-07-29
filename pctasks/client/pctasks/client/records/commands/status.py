from typing import Optional

import click


@click.command("workflow")
@click.argument("run_id")
@click.option("-d", "--dataset", help="Filter by dataset.")
@click.option(
    "-w",
    "--watch",
    is_flag=True,
    help="Watch the status of the workflow, refreshing every second",
)
@click.pass_context
def fetch_workflow_cmd(
    ctx: click.Context, run_id: str, dataset: Optional[str], watch: bool
) -> None:
    """Fetch a workflow status."""
    from . import _status

    ctx.exit(_status.workflow_status_cmd(ctx, run_id, dataset, watch))


@click.group("status")
def status_cmd() -> None:
    """Fetch a workflow status."""
    pass


status_cmd.add_command(fetch_workflow_cmd)