from cStringIO import StringIO

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage


def convert_pdf_to_txt(file_path, page_indices=[],
                       password="",
                       codec='utf-8',
                       laparams=LAParams(),
                       maxpages=0,
                       caching=True):
    """This parses pdf using PDFMINER and returns text as string"""
    retstr, device = None, None
    try:
        pagenos = set(page_indices)
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        with file(file_path, 'rb') as fp:
            for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching,
                                          check_extractable=True):
                interpreter.process_page(page)

        text = retstr.getvalue()
    finally:
        if retstr:
            retstr.close()
        if device:
            device.close()

    return text
