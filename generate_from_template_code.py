#!/usr/bin/env python3

from inspect import cleandoc
import pathlib


class TextCooker:
    def __init__(self, orig_text: str, template_str: str, threaded_str: str, async_str: str, threaded_mod_name: str, async_mod_name: str, mode: str):
        self.orig_text = orig_text
        self.template_str = template_str
        self.threaded_str = threaded_str
        self.async_str = async_str
        self.threaded_mod_name = threaded_mod_name
        self.async_mod_name = async_mod_name
        if mode not in (self.threaded_str, self.async_str):
            raise ValueError(f'Invalid mode of {mode}')
        self.mode = mode
        self.current_text = self.orig_text
        self.group_mode = None

    def global_replace_preserve_case(self, from_text, to_text):
        self.global_replace(from_text, to_text)
        self.global_replace(from_text.lower(), to_text.lower())
        self.global_replace(from_text.upper(), to_text.upper())
        self.global_replace(from_text.title(), to_text.title())
    
    def global_replace(self, from_text, to_text):
        self.current_text = self.current_text.replace(from_text, to_text)

    def get_processed(self):
        self.do_simple_replacements()
        self.do_line_processing()
        self.current_text += '\n'  # add new line at the end
        self.add_heading()  # at the end so we can use _template_ in the text.
        return self.current_text
    
    def add_heading(self):
        heading = cleandoc("""
                    # -----------------------------------------------------------------------
                    # WARNING: This file is auto-generated and should not be edited manually!
                    #
                    # It is generated using 'generate_from_template_code.py' using the
                    # _template_ directory as the source.
                    # -----------------------------------------------------------------------
                  """)
        self.current_text = heading + '\n' * 2 + self.current_text

    def do_simple_replacements(self):
        dest_str = self.mode
        mod_dest = self.threaded_mod_name if self.mode == self.threaded_str else self.async_mod_name
        self.global_replace(self.template_str + ' import', mod_dest + ' import')  # FIXME: bit hacky - use re?
        self.global_replace_preserve_case(self.template_str, dest_str)
        if self.mode == self.threaded_str:
            # FIXME: would be better to parse each line, at least enough to ignore comments
            self.global_replace('async def', 'def')
            self.global_replace('async with', 'with')
            self.global_replace('async for', 'for')
            self.global_replace(' anext(', ' next(')
            self.global_replace('await ', '')
            self.global_replace('AsyncIterable', 'Iterable')
            self.global_replace('StopAsyncIteration', 'StopIteration')
            self.global_replace('__aiter__', '__iter__')
            self.global_replace('__anext__', '__next__')

    def do_line_processing(self):
        out_lines = list()
        for line_no, raw_line in enumerate(self.current_text.splitlines()):

            parts = raw_line.rsplit('#=', 1)
            if len(parts) == 1:
                if self.group_mode:
                    if self.group_mode == self.mode:
                        out_lines.append(raw_line)
                else:
                    out_lines.append(raw_line)
                continue

            line = parts[0].rstrip()
            target_flag_parts = parts[1].split()

            # skip any line that does not have a valid target and flag
            skip_processing = False
            if len(target_flag_parts) > 2:
                skip_processing = True

            target = target_flag_parts[0]
            flag = target_flag_parts[1] if len(target_flag_parts) == 2 else None
            if target not in (self.async_str, self.threaded_str, 'remove'):
                skip_processing = True

            if flag and flag not in ('<', 'start', 'end'):
                skip_processing = True

            if skip_processing:
                out_lines.append(raw_line)
                continue

            try:
                out_line = self.process_line(line, target, flag)
                if out_line:
                    out_lines.append(out_line)
            except ValueError:
                print(f'*** Value Error in line {line_no} ***')
                raise
        self.current_text = '\n'.join(out_lines)

    def process_line(self, line, target, flag):
        if flag in ('start', 'end') and line.strip():
            raise ValueError('start and stop lines must not have any other text')
        if flag == 'start':
            if self.group_mode:
                raise ValueError(f'Start found for target {target} but already in group_mode for {self.group_mode}')
            else:
                self.group_mode = target
                return None
        if flag == 'end':
            if not self.group_mode:
                raise ValueError(f'Stop found for target {target} but not group_mode not active')
            else:
                self.group_mode = None
                return None
        if target == 'remove':
            return None
        if self.group_mode:
            # no other flags besides remove are valid in group mode
            if target:
                raise ValueError(f'Invalid target {target} in group mode')
            # if self.group_mode == self.mode:
            #     return line
            # else:
            #     return None
        if target == self.mode:
            if flag == '<':
                if line.startswith(' '*4):
                    return line[4:]
                else:
                    raise ValueError('< flag used but line not indented')
            else:
                return line


class ThreadedFromAsyncUpdater:
    def __init__(self, template_dir, async_str='async', threaded_str='threaded', template_str='_template_', async_mod_name = 'asyncio', threaded_mod_name='threaded'):
        self.template_dir = pathlib.Path(template_dir)
        self.async_mod_name = async_mod_name
        self.threaded_mod_name = threaded_mod_name
        self.asyc_dir = self.template_dir.parent / async_mod_name
        self.theaded_dir = self.template_dir.parent / threaded_mod_name
        self.template_str = template_str
        self.async_str = async_str
        self.threaded_str = threaded_str

    def update_from_template_folder(self):
        for rel_path in self.get_template_files():
            self.generate_async(rel_path)
            self.generate_threaded(rel_path)

    def get_src(self, rel_path):
        return self.template_dir / rel_path
    
    def generate_async(self, rel_path):
        src = self.get_src(rel_path)
        dst = self.asyc_dir / rel_path
        print(src, '-->', dst)        
        orig = open(src, newline='\n').read()
        try:
            cooked = self.cook(orig, self.async_str)
        except ValueError:
            print(f'*** Error in file {src} ***')
            raise
        dst.parent.mkdir(exist_ok=True)
        with open(dst, 'w', newline='\n') as f:
            f.write(cooked)

    def generate_threaded(self, rel_path):
        assert isinstance(rel_path, pathlib.Path)
        src = self.get_src(rel_path)
        dst = self.theaded_dir / rel_path
        print(src, '-->', dst)
        orig = open(src, newline='\n').read()
        cooked = self.cook(orig, self.threaded_str)
        dst.parent.mkdir(exist_ok=True)
        with open(dst, 'w', newline='\n') as f:
            f.write(cooked)

    def cook(self, orig_text: str, mode: str):
        cooker = TextCooker(orig_text, self.template_str, self.threaded_str, self.async_str, self.threaded_mod_name, self.async_mod_name, mode)
        return cooker.get_processed()
    
    def get_template_files(self):
        for root, dirs, files in pathlib.Path(self.template_dir).walk():
            if root.name == '__pycache__':
                continue
            for filename in files:
                path = root / filename
                if path.suffix != '.py':
                    continue
                yield path.relative_to(self.template_dir)
                


def main():
    root = pathlib.Path(__file__).parent
    template_dir = root / 'src' / 'sparrowrpc' / '_template_'
    updater = ThreadedFromAsyncUpdater(template_dir)
    updater.update_from_template_folder()


if __name__ == '__main__':
    main()
