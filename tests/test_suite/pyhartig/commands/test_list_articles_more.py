import subprocess
import os
from pathlib import Path
import types
import pytest

import pyhartig.commands.list_articles as la_mod


def test_execute_uses_shell_fetch_success(tmp_path, monkeypatch):
    mapping_dir = tmp_path / 'usecase'
    mapping_dir.mkdir()
    mapping_file = mapping_dir / 'mapping.ttl'
    mapping_file.write_text('# mapping')
    fetch_script = mapping_dir / 'fetch_all.sh'
    fetch_script.write_text('#!/bin/sh')

    fake = types.SimpleNamespace(ran=None, written=[], updated=None)
    fake._write_json = lambda data, outpath: fake.written.append((str(outpath), data))
    fake._update_env_file = lambda env_file, updates: setattr(fake, 'updated', (str(env_file), dict(updates)))
    fake._apply_year_filter = lambda raw, s, a, b: raw
    fake._run_mapping_on_dir = lambda m, o, a: setattr(fake, 'ran', (m, o, a))

    monkeypatch.setattr(la_mod, 'MappingRunCommand', lambda: fake)

    # simulate subprocess.run success on first shell invocation
    def fake_run(cmd, *args, **kwargs):
        class R: stdout = 'ok'; stderr = ''
        return R()

    monkeypatch.setattr(la_mod.subprocess, 'run', fake_run)

    args = types.SimpleNamespace(author='A', mapping=str(mapping_file), outdir=None, sources='openalex', start_year=None, end_year=None)
    cmd = la_mod.ListArticlesCommand()
    cmd.execute(args)
    assert fake.ran is not None


def test_execute_fallback_shell_commands(monkeypatch, tmp_path):
    mapping_dir = tmp_path / 'usecase'
    mapping_dir.mkdir()
    mapping_file = mapping_dir / 'mapping.ttl'
    mapping_file.write_text('# mapping')
    fetch_script = mapping_dir / 'fetch_all.sh'
    fetch_script.write_text('#!/bin/sh')

    fake = types.SimpleNamespace(ran=None, written=[], updated=None)
    fake._write_json = lambda data, outpath: fake.written.append((str(outpath), data))
    fake._update_env_file = lambda env_file, updates: setattr(fake, 'updated', (str(env_file), dict(updates)))
    fake._apply_year_filter = lambda raw, s, a, b: raw
    fake._run_mapping_on_dir = lambda m, o, a: setattr(fake, 'ran', (m, o, a))
    monkeypatch.setattr(la_mod, 'MappingRunCommand', lambda: fake)

    calls = []

    def fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        # first call mimics shell=True call raising CalledProcessError
        if isinstance(cmd, str):
            raise subprocess.CalledProcessError(returncode=1, cmd=cmd, output='fail')
        # second call will succeed
        class R: stdout = 'ok'; stderr = ''
        return R()

    monkeypatch.setattr(la_mod.subprocess, 'run', fake_run)

    args = types.SimpleNamespace(author='A', mapping=str(mapping_file), outdir=None, sources='openalex', start_year=None, end_year=None)
    cmd = la_mod.ListArticlesCommand()
    cmd.execute(args)
    # ensure both an initial string-run and a list-run were attempted
    assert any(isinstance(c, str) for c in calls)
    assert any(isinstance(c, list) for c in calls)


def test_unknown_source_logged(caplog, tmp_path, monkeypatch):
    mapping_dir = tmp_path / 'usecase'
    mapping_dir.mkdir()
    mapping_file = mapping_dir / 'mapping.ttl'
    mapping_file.write_text('# mapping')

    fake = types.SimpleNamespace(ran=None, written=[], updated=None)
    fake._write_json = lambda data, outpath: fake.written.append((str(outpath), data))
    fake._update_env_file = lambda env_file, updates: setattr(fake, 'updated', (str(env_file), dict(updates)))
    fake._apply_year_filter = lambda raw, s, a, b: raw
    fake._run_mapping_on_dir = lambda m, o, a: setattr(fake, 'ran', (m, o, a))
    monkeypatch.setattr(la_mod, 'MappingRunCommand', lambda: fake)

    # ensure fetch script exists so code runs the fetch-script branch then falls back
    fetch_script = mapping_dir / 'fetch_all.sh'
    fetch_script.write_text('#!/bin/sh')

    # make subprocess.run always raise CalledProcessError so ran_script stays False
    def always_fail(cmd, *a, **kw):
        raise subprocess.CalledProcessError(returncode=1, cmd=cmd)

    monkeypatch.setattr(la_mod.subprocess, 'run', always_fail)

    args = types.SimpleNamespace(author='A', mapping=str(mapping_file), outdir=None, sources='weird', start_year=None, end_year=None)
    caplog.set_level('INFO')
    cmd = la_mod.ListArticlesCommand()
    cmd.execute(args)
    assert any('Unknown source' in r.message for r in caplog.records)
