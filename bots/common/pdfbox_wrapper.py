"""This module is a wrapper of pdfbox, which enables extract text/image from PDF.

Currently, the implementation of this module uses subprocess.

"""

import os
import shlex
import subprocess
import sys

import requests

JAR_NAME = "pdfbox-app-2.0.7.jar"
JAR_DIR = os.path.abspath(os.path.join(os.path.realpath(__file__), "../../../lib/"))
JAR_PATH = os.path.join(JAR_DIR, JAR_NAME)


# ExtractText -ignoreBeads -startPage 1 -endPage 1
def parse_text(input_path, start_page=1, end_page=sys.maxint, encoding=None, java_options=None, **kwargs):
    """Extracts text from PDF returns String.

    Args:
        input_path (str):
            File path of tareget PDF file.
        output_path (str):
            File path of output file.
        start_page (int, optional):
            The first page to extract, one based. Default: 1
        end_page (int, optional):
            The last page to extract, one based. Default: sys.maxint
        java_options (list, optional):
            Set java options like `-Xmx256m`.
        kwargs (dict):
            Dictionary of option for pdfbox. Details are shown in `build_options()`

    Returns:
        string
    """

    # if output_path is None or len(output_path) is 0:
    #     raise AttributeError("'output_path' shoud not be None or empty")

    kwargs['startPage'] = start_page or kwargs.get('startPage', None)
    kwargs['endPage'] = end_page or kwargs.get('endPage', None)
    kwargs['encoding'] = encoding or kwargs.get('encoding', None)

    if java_options is None:
        java_options = []

    elif isinstance(java_options, str):
        java_options = [java_options]

    options = build_options(kwargs)
    path, is_url = localize_file(input_path)
    args = ["java"] + java_options + ["-jar", JAR_PATH] + options + [path]

    try:
        return subprocess.check_output(args)
    finally:
        if is_url:
            os.unlink(path)


def localize_file(path):
    """Ensure localize target file.

    If the target file is remote, this function fetches into local storage.

    Args:
        path (str):
            File path or URL of target file.
    """

    is_url = False
    try:
        pid = os.getpid()
        r = requests.get(path)
        filename = os.path.basename(r.url)
        if os.path.splitext(filename)[-1] is not ".pdf":
            filename = "{0}.pdf".format(pid)

        with open(filename, 'wb') as f:
            f.write(r.content)

        is_url = True
        return filename, is_url

    except:
        return path, is_url


def build_options(kwargs=None):
    """Build options for tabula-java

    Args:
        https://pdfbox.apache.org/2.0/commandline.html
    Returns:
        Built dictionary of options
    """
    __options = []
    if kwargs is None:
        kwargs = {}
    options = kwargs.get('options', '')
    # handle options described in string for backward compatibility
    __options += shlex.split(options)

    __options.append('ExtractText')

    # parse options
    start_page = kwargs.get('startPage', 1)
    if start_page:
        if isinstance(start_page, int):
            start_page = str(start_page)
        __options += ["-startPage", start_page]

    end_page = kwargs.get('endPage', sys.maxint)
    if end_page:
        if isinstance(end_page, int):
            end_page = str(end_page)
        __options += ["-endPage", end_page]
    encoding = kwargs.get('encoding')
    if encoding:
        __options += ["-encoding", encoding]

    ignore_beads = kwargs.get('ignoreBeads')
    if ignore_beads:
        __options.append("-ignoreBeads")

    __options.append("-console")
    return __options
