from setuptools import setup

setup(
    name='nitro',
    version='0.1',
    py_modules=['nitro'],
    install_requires=[
        'Click',
        'colorama',
        'mpi4py'
    ],
    entry_points='''
        [console_scripts]
        nitro=nitro:cli
    ''',
)