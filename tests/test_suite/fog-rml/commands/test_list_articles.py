import os
from types import SimpleNamespace
from argparse import Namespace
from pathlib import Path

import pytest

import fog_rml.commands.list_articles as la_mod


class FakeRun:
    def __init__(self):
        self.written = []
        self.updated = None
        self.ran = None

    def _write_json(self, data, outpath):
        # record write but do not actually persist
        self.written.append((str(outpath), data))

    def _update_env_file(self, env_file, updates):
        self.updated = (str(env_file), dict(updates))

    def _load_env_file(self, env_file):
        return {}

    def _apply_year_filter(self, raw, src_name, start, end):
        return raw

    def _run_mapping_on_dir(self, mapping_path, outdir, author):
        self.ran = (mapping_path, outdir, author)


import logging


def test_fetch_serpapi_skips_without_key(caplog):
    cmd = la_mod.ListArticlesCommand()
    fake = FakeRun()
    # monkeypatch MappingRunCommand to return our fake
    la_mod.MappingRunCommand = lambda: fake

    # call _fetch_serpapi with no env var set
    os.environ.pop('SERPAPI_API_KEY', None)
    os.environ.pop('SERPAPI_KEY', None)
    caplog.set_level(logging.INFO)
    cmd._fetch_serpapi(fake, 'Some Author', None, None, Path('doesnotmatter'))
    assert any('SERPAPI key not found' in rec.message for rec in caplog.records)


def test_execute_creates_data_dir_and_runs_mapping(tmp_path, monkeypatch):
    # create a fake mapping file and ensure data dir behavior
    mapping_dir = tmp_path / 'usecase'
    mapping_dir.mkdir()
    mapping_file = mapping_dir / 'mapping.ttl'
    mapping_file.write_text('# dummy mapping')

    # ensure a fetch_all.sh exists to avoid a None fetch_script in execute
    (mapping_dir / 'fetch_all.sh').write_text('#!/bin/sh')

    fake = FakeRun()
    monkeypatch.setattr(la_mod, 'MappingRunCommand', lambda: fake)

    # replace network fetchers with no-op functions that write minimal JSON via fake
    def fake_fetch(self, run_cmd, author, start, end, outpath):
        run_cmd._write_json({'results': []}, outpath)

    monkeypatch.setattr(la_mod.ListArticlesCommand, '_fetch_openalex', fake_fetch)
    monkeypatch.setattr(la_mod.ListArticlesCommand, '_fetch_hal', fake_fetch)
    monkeypatch.setattr(la_mod.ListArticlesCommand, '_fetch_dblp', fake_fetch)
    monkeypatch.setattr(la_mod.ListArticlesCommand, '_fetch_serpapi', fake_fetch)

    args = Namespace(author='Test Author', mapping=str(mapping_file), outdir=None, sources='openalex,hal,dblp,serpapi', start_year=None, end_year=None)

    cmd = la_mod.ListArticlesCommand()
    # should not raise
    cmd.execute(args)

    # fake run should have recorded a mapping run
    assert fake.ran is not None
    # .env should have been updated
    assert fake.updated is not None

