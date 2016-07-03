import os

import click

from .pefs import Pefs


@click.group()
def cli():
    pass


@cli.command()
@click.argument('db')
@click.argument('efs-root')
@click.option('--pg-user')
def clone(db, efs_root, pg_user):
    pefs = Pefs(db, 'public', efs_root, pg_user, '')
    pefs.clone_db()


@cli.command()
@click.argument('db')
@click.argument('efs-root')
@click.option('--pg-user')
def link(db, efs_root, pg_user):
    pefs = Pefs(db, 'public', efs_root, pg_user, '')
    pefs.link_db()


if __name__ == '__main__':
    cli()
