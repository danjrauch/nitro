import click
import subprocess
import os
import json
import glob
import shutil
from mpi4py import MPI
from graph import Graph
from collections import defaultdict
from simple_salesforce import Salesforce
from ttictoc import TicToc
from constraints import *

def command_required_option_from_option(master, requires):
    class CommandOptionRequiredClass(click.Command):
        def invoke(self, ctx):
            if ctx.params[master] is not None:
                for require in requires:
                    if ctx.params[require.lower()] is None:
                        raise click.ClickException('With {}={} must specify option --{}'.format(master, ctx.params[master], require))
            super(CommandOptionRequiredClass, self).invoke(ctx)
    return CommandOptionRequiredClass


@click.group(invoke_without_command=True)
@click.pass_context
# @click.argument('name')
def cli(ctx):
    """Nitro Main Command."""
    if ctx.invoked_subcommand is None:
        click.echo(click.style('Hello, call nitro with a command', fg = 'bright_blue'))


@cli.command()
@click.argument('dir', type=click.Path(exists=True, dir_okay=True))
@click.argument('n_proc')
def validate(dir, n_proc):
    click.secho('I am about to invoke validate on {0}'.format(dir), fg = 'bright_red')

    with TicToc('Satisfied Constraints'):
        subprocess.call(['mpiexec', '-n', str(n_proc), 'python', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'constraints.py'), dir])
        # -m mpi4py


@cli.command(cls=command_required_option_from_option('username', ['password', 'security_token', 'domain']))
@click.argument('dir', type=click.Path(exists=True, dir_okay=True))
@click.option('--n_proc', '-n', default=1)
@click.option('--username', '-u')
@click.option('--password', '-p', hide_input=True)
@click.option('--security_token', '-st')
@click.option('--domain', '-d', type=click.Choice(['test', 'prod']))
@click.option('--make_default', '-md', is_flag=True)
def run(dir, n_proc, username, password, security_token, domain, make_default):
    click.secho('Validating and inserting files in {0}'.format(dir), fg = 'bright_red')

    credential_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'credentials.cfg')

    if not os.path.exists(credential_filepath):
        with open(credential_filepath, 'w') as file:
            file.write('')

    creds = {}
    if username is None:
        with open(credential_filepath, 'r') as file:
            credential_lines = file.read().splitlines()
            if len(credential_lines) == 1:
                creds = dict(item.split('=') for item in credential_lines[0].split(';'))
    else:
        creds = {
            'username': username,
            'password': password,
            'security_token': security_token,
            'domain': domain
        }

    if creds is None:
        print('Could not create credentials for Salesforce. Aborting.')
        sys.exit(1)

    if make_default:
        with open(credential_filepath, 'w') as file:
            file.write('username={0};password={1};security_token={2};domain={3}'.format(creds['username'],
                                                                                        creds['password'],
                                                                                        creds['security_token'],
                                                                                        creds['domain']))

    sf = Salesforce(username=creds['username'],
                    password=creds['password'],
                    security_token=creds['security_token'],
                    client_id='nitro',
                    domain=creds['domain'])

    while True:
        objs = sf.bulk.Opportunity.query('SELECT Id FROM Opportunity LIMIT 10000')
        objs = [{i:obj[i] for i in obj if i!='attributes'} for obj in objs]
        if len(objs) == 0:
            break
        sf.bulk.Opportunity.delete(objs)
        print('deleted {0} Opportunities'.format(len(objs)))
    while True:
        objs = sf.bulk.Contact.query('SELECT Id FROM Contact LIMIT 10000')
        objs = [{i:obj[i] for i in obj if i!='attributes'} for obj in objs]
        if len(objs) == 0:
            break
        sf.bulk.Contact.delete(objs)
        print('deleted {0} Contacts'.format(len(objs)))
    while True:
        objs = sf.bulk.Account.query('SELECT Id FROM Account LIMIT 10000')
        objs = [{i:obj[i] for i in obj if i!='attributes'} for obj in objs]
        if len(objs) == 0:
            break
        sf.bulk.Account.delete(objs)
        print('deleted {0} Accounts'.format(len(objs)))
    while True:
        objs = sf.bulk.Lead.query('SELECT Id FROM Lead LIMIT 10000')
        objs = [{i:obj[i] for i in obj if i!='attributes'} for obj in objs]
        if len(objs) == 0:
            break
        sf.bulk.Lead.delete(objs)
        print('deleted {0} Leads'.format(len(objs)))
    while True:
        objs = sf.bulk.Task.query('SELECT Id FROM Task LIMIT 10000')
        objs = [{i:obj[i] for i in obj if i!='attributes'} for obj in objs]
        if len(objs) == 0:
            break
        sf.bulk.Task.delete(objs)
        print('deleted {0} Tasks'.format(len(objs)))
    while True:
        objs = sf.bulk.Campaign.query('SELECT Id FROM Campaign LIMIT 10000')
        objs = [{i:obj[i] for i in obj if i!='attributes'} for obj in objs]
        if len(objs) == 0:
            break
        sf.bulk.Campaign.delete(objs)
        print('deleted {0} Campaigns'.format(len(objs)))
    while True:
        objs = sf.bulk.Contract.query('SELECT Id FROM Contract LIMIT 10000')
        objs = [{i:obj[i] for i in obj if i!='attributes'} for obj in objs]
        if len(objs) == 0:
            break
        sf.bulk.Contract.delete(objs)
        print('deleted {0} Contracts'.format(len(objs)))

    with TicToc('Run'):

        with TicToc('Satisfied Constraints'):
            subprocess.call(['mpiexec', '-n', str(n_proc), 'python', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'constraints.py'), dir])
            # -m mpi4py

        with TicToc('Scheduling'):
            # create schedule
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'relationship.json')) as relationships:
                data = json.load(relationships)
                G = Graph(len(data.keys()))
                node_map = {}
                rev_node_map = {}
                node_idx = 0
                for file_name in data:
                    if file_name not in node_map:
                        node_map[file_name] = node_idx
                        rev_node_map[node_idx] = file_name
                        node_idx += 1
                for file_name in data:
                    for reference in data[file_name]['children']:
                        G.addEdge(node_map[reference['object']], node_map[file_name])
                schedule = [rev_node_map[node] for node in G.topologicalSort()]
                schedule = [[x, 0] for x in schedule]
                for i, x in enumerate(schedule):
                    for j in range(i+1, len(schedule)):
                        if node_map[schedule[j][0]] in G.neighbors(node_map[x[0]]):
                            if x[1] + 1 > schedule[j][1]:
                                schedule[j][1] = x[1] + 1

        with TicToc('Inserting Files'):
            # start the supervisor
            os.chdir(dir)
            sf.Account.create({'Name':'Administrative'})
            accountId = sf.query('SELECT Id FROM Account LIMIT 1')['records'][0]['Id']
            with open('Contract.csv', 'r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames + ['AccountId']
                new_rows = []
                for row_num, row in enumerate(reader):
                    row.update({'AccountId': accountId})
                    new_rows.append(row)
            with open('Contract.csv', 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(new_rows)
            os.chdir(os.path.dirname(os.path.abspath(__file__)))

            for i in range(0, schedule[-1][1]+1):
                names = ','.join([x[0] for x in schedule if x[1] == i])
                subprocess.call(['mpiexec', '-n', str(n_proc), 'python', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'insert.py'), dir, names])

    os.chdir(dir)
    files = glob.glob('*.{}'.format('csv'))
    for file in files:
        os.remove(file)
        shutil.copyfile(os.path.join('archive', file), os.path.join('.', file))