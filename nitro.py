import click
import subprocess
import os
import glob
from ctypes import CDLL, c_int, c_char_p

@click.group(invoke_without_command=True)
@click.pass_context
# @click.argument('name')
def cli(ctx):
    """Nitro Main Command."""
    if ctx.invoked_subcommand is None:
        click.echo(click.style('Hello, call nitro with a command', fg='bright_blue'))

@cli.command()
@click.pass_context
@click.argument('dir', type=click.Path(exists=True, dir_okay=True))
def validate(ctx, dir):
    click.secho('I am about to invoke {0} on {1}'.format('validate', dir), fg='bright_red')

    subprocess.call(['gcc','-shared','-Wl,-install_name,applib.so','-o','applib.so','-fPIC','app.c'])
    lib = CDLL('./applib.so')

    os.chdir(dir)
    files = glob.glob('*.{}'.format('csv'))

    lib.validate.restype = c_int
    lib.validate.argtypes = [(c_char_p * len(files)), c_int, (c_char_p * 10), c_int]

    files_input = (c_char_p * len(files))()
    for key, item in enumerate(files):
        files_input[key] = item.encode()
    errors = (c_char_p*10)()

    result = lib.validate(files_input, len(files_input), errors, len(errors))

    print(result)
    # for res in errors:
    #     print(res.decode('utf-8'))

    # with click.progressbar([i for i in range(100000)]) as bar:
    #     for i in bar:
    #         i + 5

    os.chdir('..')

    subprocess.call(['mpiexec','-n','2','python','import.py'])