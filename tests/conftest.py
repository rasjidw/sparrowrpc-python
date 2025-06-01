import os
import pathlib
import sys
import subprocess
import time

import listening_server_code



class ListeningServerRunner:
    def __init__(self, pyfile_path, stdout_filenane=None, stderr_filename=subprocess.STDOUT):
        self.pyfile_path = pyfile_path
        self.sub_proc = None
        if not stdout_filenane:
            stdout_filenane = pathlib.Path(pyfile_path).stem + '.stdout.txt'
        self.stdout_filename = stdout_filenane
        if not stderr_filename:
            stderr_filename = pathlib.Path(pyfile_path).stem + '.stderr.txt'
        self.stderr_filename = stderr_filename
        self.f_out = None
        self.f_err = None

    def start(self):
        if not self.sub_proc:
            args = [sys.executable, '-u', self.pyfile_path]
            self.f_out = open(self.stdout_filename, 'w')
            if self.stderr_filename != subprocess.STDOUT:
                self.f_err = open(self.stderr_filename, 'w')
            self.sub_proc = subprocess.Popen(args, stdout=self.f_out, stderr=self.f_err)
            print(f'** Starting subprocess with pid {self.sub_proc.pid}')

    def stop(self):
        if self.sub_proc:
            try:
                self.sub_proc.terminate()
                try:
                    rc = self.sub_proc.wait(timeout=5)
                except Exception:
                    print(f'Killing subprocess {self.sub_proc.pid}')
                    self.sub_proc.kill()
                    rc = self.sub_proc.wait()
                print(f'** Got exit code of {rc} from subprocess {self.sub_proc.pid}')
            finally:
                self.sub_proc = None
                self.f_out.close()
                if self.f_err:
                    self.f_err.close()


listening_server_runner = ListeningServerRunner(listening_server_code.__file__)


def pytest_configure(config):
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
    print('In PyTest Configure')
    listening_server_runner.start()


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    print('In PyTest Session Start')
    

def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    print('In PyTest Session End')
    

def pytest_unconfigure(config):
    """
    called before test process is exited.
    """
    print('In PyTest Unconfigure')
    listening_server_runner.stop()
