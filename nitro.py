import click
import subprocess
import os
from ttictoc import TicToc
from constraints import *

@click.group(invoke_without_command=True)
@click.pass_context
# @click.argument('name')
def cli(ctx):
    """Nitro Main Command."""
    if ctx.invoked_subcommand is None:
        click.echo(click.style('Hello, call nitro with a command', fg = 'bright_blue'))

@cli.command()
@click.argument('dir', type=click.Path(exists=True, dir_okay=True))
def validate(dir):
    click.secho('I am about to invoke validate on {0}'.format(dir), fg = 'bright_red')

    with TicToc('Satisfied Constraints'):
        subprocess.call(['mpiexec', '-n', '4', 'python', 'constraints.py', dir])
        # -m mpi4py