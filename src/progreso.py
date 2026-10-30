"""Barras de progreso y cronómetros para la consola."""

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


def crear_progreso() -> Progress:
    """Progreso con spinner, barra, porcentaje, tiempo transcurrido y tiempo restante."""
    return Progress(
        SpinnerColumn("line"),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )
