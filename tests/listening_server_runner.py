import pathlib
import sys
import subprocess
import time


class ListeningServerRunner:
    def __init__(self, pyfile_path, proc_arg_list, stdout_filenane=None, stderr_filename=subprocess.STDOUT):
        self.pyfile_path = pyfile_path
        self.proc_arg_list = proc_arg_list
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
            args = [sys.executable, '-u', self.pyfile_path] + self.proc_arg_list
            self.f_out = open(self.stdout_filename, 'w')
            if self.stderr_filename != subprocess.STDOUT:
                self.f_err = open(self.stderr_filename, 'w')
            self.sub_proc = subprocess.Popen(args, stdout=self.f_out, stderr=self.f_err)
            print(f'** Starting subprocess with pid {self.sub_proc.pid}')
            print(f'Sleeping to enable the listening socket to appear')
            time.sleep(2)

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
