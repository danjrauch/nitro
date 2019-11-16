import click
import subprocess
import os
import glob
from ttictoc import TicToc
from validate import *

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

    os.chdir(dir)
    files = glob.glob('*.{}'.format('csv'))

    with TicToc('Constraint Keys'):
        result = constraint_keys(files)

    # print(result)
    # for res in errors:
    #     print(res.decode('utf-8'))

    # with click.progressbar([i for i in range(100000)]) as bar:
    #     for i in bar:
    #         i + 5