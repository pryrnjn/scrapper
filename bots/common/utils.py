import HTMLParser
import copy
import csv
import logging
import os
import time

from pdfbox_wrapper import parse_text

logger = logging.getLogger(__name__)

WHITESPACE = ' '
EMPTY_STRING = ''
ASTERISK = '*'
HASH = '#'
DOUBLESPACE = WHITESPACE * 2
NEW_LINE, CARRIAGE_RETURN, NEW_TAB = '\n', '\r', '\t'
SPECIAL_CHARS = [NEW_LINE, CARRIAGE_RETURN, NEW_TAB]
TRADEMARK_SYMBOL = u'\u2122'.encode('utf-8')
TRADEMARK_TEXT = 'TM'


def write_to_csv_from_json(file_path, fieldnames, items, dialect='excel'):
    try:
        with open(file_path, 'wb') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames, dialect=dialect)
            writer.writeheader()
            writer.writerows(items)
            logger.debug("Successfully wrote csv at %(path)r", {'path': file_path})
    except Exception as e:
        raise ValueError(
            "Couldn't write all/some of the rows to the csv at %s" % file_path, e)


def trim(text):
    if is_type_text(text):
        if type(text) is not unicode:
            text = text.decode('utf-8')
        text = text.replace(u'\xa0', WHITESPACE)
        return text.strip().encode('ascii', 'ignore')
    return text


def html_decode(text):
    """
    Decode the texts' html encoding like &amp; to &
    :param text:
    :return: decoded text
    """
    if is_type_text(text):
        text = HTMLParser.HTMLParser().unescape(text)
        text = text.encode('utf-8')
    return text


def is_str_equals(str1, str2, ignorecase=False, ignore_chars=[]):
    if ignorecase:
        str1 = str1.lower()
        str2 = str2.lower()
    if ignore_chars:
        str1 = remove_special_chars(str1, special_chars=ignore_chars, replace_with=EMPTY_STRING)
        str2 = remove_special_chars(str2, special_chars=ignore_chars, replace_with=EMPTY_STRING)
    return str1 == str2


def extract_text(element, xpath='.//text()', join_list=True, first_ele=False):
    arr = []
    for val in element.xpath(xpath).extract():
        val = trim(val)
        if val:
            arr.append(val)
    if join_list:
        return WHITESPACE.join(arr)
    elif first_ele:
        return arr[0] if arr else EMPTY_STRING
    else:
        return arr


def extract_int(element, xpath, default=0):
    try:
        val = element.xpath(xpath).extract_first()
        val = int(val) if val else default
    except ValueError:  # not an int assigning default value
        val = default
    except:  # invalid xpath provided
        raise
    return val


def assign_item(headers_dict, local_headers, row, item_class, split_on=None):
    item = item_class()
    for i in range(len(local_headers)):
        local_header = local_headers[i]
        item[get_item_key(headers_dict, local_header)] = row[i] if len(row) > i else None
    if split_on and type(item.get(split_on)) is list and len(item.get(split_on)) > 1:
        split_on_arr = item[split_on]
        for val in split_on_arr:
            new_item = copy.deepcopy(item)
            new_item[split_on] = val
            yield new_item
    else:
        yield item


def get_item_key(headers_dict, header):
    for dict_key in headers_dict.keys():
        if is_str_equals(header, dict_key, ignorecase=True, ignore_chars=[WHITESPACE, ASTERISK, HASH]):
            return headers_dict.get(dict_key)
    raise ValueError("Key %s Not found" % header)


def remove_special_chars(text, special_chars=SPECIAL_CHARS, replace_with=WHITESPACE):
    if is_type_text(text):
        for special_char in special_chars:
            text = text.replace(special_char, replace_with)
    return text


def is_type_text(text):
    return type(text) in [str, unicode]


def get_dir_name(file_path):
    return os.path.dirname(file_path)


def remove_rows_csv(file_path, to_be_removed_row_indices=set(), remove_blank_rows=True, remove_headers=[],
                    remove_rows_containing_text=None):
    to_be_removed_row_indices = set(to_be_removed_row_indices) if type(to_be_removed_row_indices) in [
        list] else to_be_removed_row_indices
    dir_name = get_dir_name(file_path)
    out_file_path = os.path.join(dir_name, "%d.csv" % time.time())
    try:
        with open(file_path, 'rb') as in_file, open(out_file_path, 'wb') as out_file:
            writer = csv.writer(out_file)
            row_idx = 0

            for row in csv.reader(in_file):
                if row_idx not in to_be_removed_row_indices and \
                        (not remove_blank_rows or any(field.strip() for field in row)) and \
                        not is_header_row_in_any_order(row, remove_headers) and \
                        not does_row_contains_text(row, remove_rows_containing_text):
                    writer.writerow(row)
                else:
                    logger.debug('skipping: {row}'.format(row=row))
                row_idx += 1
        move_file(out_file_path, file_path)
        logger.debug("Successfully cleaned up rows from csv at %(path)r", {'path': file_path})
    except Exception as e:
        raise ValueError(
            "cleanup_blank_rows_csv failed for the csv at %s" % file_path, e)


def remove_columns_csv(file_path, to_be_removed_col_indices=set(), remove_blank_columns=True):
    to_be_removed_col_indices = set(to_be_removed_col_indices) if type(to_be_removed_col_indices) in [
        list] else to_be_removed_col_indices
    dir_name = get_dir_name(file_path)
    out_file_path = os.path.join(dir_name, "%d.csv" % time.time())
    try:
        with open(file_path, 'rb') as in_file, open(out_file_path, 'wb') as out_file:
            column_wise_data = []
            num_cols = 0
            for row in csv.reader(in_file):
                num_cols = max(num_cols, len(row))

            for i in range(num_cols):
                column_wise_data.append([])
            in_file.seek(0)
            for row in csv.reader(in_file):
                for i in range(len(row)):
                    column_wise_data[i].append(row[i].strip())

            if remove_blank_columns:
                for i in range(len(column_wise_data)):
                    if i not in to_be_removed_col_indices and not any(column_wise_data[i]):
                        to_be_removed_col_indices.add(i)

            writer = csv.writer(out_file)
            in_file.seek(0)
            for row in csv.reader(in_file):
                row = remove_arr_ele(row, to_be_removed_col_indices)
                writer.writerow(row)

        move_file(out_file_path, file_path)
        logger.debug("Successfully cleaned up columns from csv at %(path)r", {'path': file_path})
    except Exception as e:
        raise


def clean_cells_csv(file_path):
    dir_name = get_dir_name(file_path)
    out_file_path = os.path.join(dir_name, "%d.csv" % time.time())
    with open(file_path, 'rb') as in_file, open(out_file_path, 'wb') as out_file:
        writer = csv.writer(out_file)
        for row in csv.reader(in_file):
            for i in range(len(row)):
                row[i] = remove_multi_spaces(remove_special_chars(row[i]))
            writer.writerow(row)

    move_file(out_file_path, file_path)


def insert_row_csv(file_path, rows=[], prepend=False, append=False):
    dir_name = get_dir_name(file_path)
    out_file_path = os.path.join(dir_name, "%d.csv" % time.time())
    try:
        with open(file_path, 'rb') as in_file, open(out_file_path, 'wb') as out_file:
            writer = csv.writer(out_file)

            if prepend:
                for row in rows:
                    writer.writerow(row)

            for row in csv.reader(in_file):
                writer.writerow(row)

            if append:
                for row in rows:
                    writer.writerow(row)

        move_file(out_file_path, file_path)
        logger.debug("""Successfully inserted rows to csv at %(path)r. 
                    Options=> prepend: %(prepend)r, append: %(append)r""",
                     {'path': file_path, 'prepend': prepend, 'append': append})
    except Exception as e:
        raise ValueError(
            """inserting rows failed for the csv at %s.
            Options=> prepend: %s, append: %s""" % (file_path, prepend, append), e)


def merge_sub_headers_csv(csv_file, final_headers):
    """Merge sub-headers and their data under a common header as passed in the same order
    Data outside the final_header will be ignored
    Assuming the rows below the sub-header is according to the sub-header
    """
    if not final_headers:
        raise AttributeError("Invalid headers passed!!")

    dir_name = get_dir_name(csv_file)
    out_file_path = os.path.join(dir_name, "%d.csv" % time.time())

    with open(csv_file, 'rb') as in_file, open(out_file_path, 'wb') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(final_headers)

        reader = csv.reader(in_file)

        copy_csv(reader, writer, final_headers)

    move_file(out_file_path, csv_file)


def copy_csv(in_file_reader, out_file_writer, final_headers):
    """This copies rows from one CSV to another respecting `final_headers` in order
        Columns other than final_headers will be ignored
        It handles even sub-headers in diff order
    """
    local_header_idx = {}
    for row in in_file_reader:
        if is_header_row_in_any_order(row, final_headers):
            local_header_idx = {}
            for i in range(len(row)):
                local_header_idx[row[i]] = i
        elif local_header_idx:
            out_row = []
            for header in final_headers:
                idx = local_header_idx.get(header, -1)
                try:
                    out_row.append(row[idx] if idx > -1 else EMPTY_STRING)
                except:
                    raise
            out_file_writer.writerow(out_row)


def merge_multiple_csv(file_list, final_file, final_headers, delete_merged_files=False):
    """Merge multiple CSVs into one CSV and their data under a common header as passed in the same order
        Data outside the final_header will be ignored
        Assuming first rows of input CSVs as their headers
    """
    if not final_headers:
        raise AttributeError("Invalid headers passed!!")
    with open(final_file, 'wb') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(final_headers)

        for f in file_list:
            with open(f, 'rb') as in_file:
                reader = csv.reader(in_file)
                copy_csv(reader, writer, final_headers)
    if delete_merged_files:
        for f in file_list:
            delete_file(f)


def replace_column_text_csv(file_path, headers, col, delimeter="", data_dict=None):
    if not data_dict:
        raise AttributeError("Invalid data_dict passed!!")
    col_idx = headers.index(col)
    if col_idx < 0:
        raise AttributeError("Invalid col, not present in Headers provided !!")

    dir_name = get_dir_name(file_path)
    out_file_path = os.path.join(dir_name, "%d.csv" % time.time())
    with open(file_path, 'rb') as in_file, open(out_file_path, 'wb') as out_file:
        writer = csv.writer(out_file)
        for row in csv.reader(in_file):
            if not is_header_row_in_any_order(row, headers):
                row[col_idx] = get_replaced_text_by_dict(row[col_idx], data_dict, delimeter)
            writer.writerow(row)

    move_file(out_file_path, file_path)


def get_replaced_text_by_dict(text, data_dict, delimeter=""):
    data_arr = text.split(delimeter) if delimeter else [text]
    for i in range(len(data_arr)):
        data_arr[i] = data_dict.get(trim(data_arr[i]), trim(data_arr[i]))
    return delimeter.join(data_arr)


def is_header_row_in_any_order(row, header_row):
    return is_arr_subset_in_any_order(row, header_row)


def is_arr_subset_in_any_order(subset, superset):
    superset_map = {}
    for ele in superset:
        superset_map[ele] = superset_map.get(ele, 0) + 1

    for ele in subset:
        superset_map[ele] = superset_map.get(ele, 0) - 1
        if superset_map.get(ele) < 0:
            return False
    return True


def is_arr_subset_in_same_order(subset, superset):
    subset_idx, superset_idx = 0, 0
    while subset_idx < len(subset):
        found_in_superset = False
        while superset_idx < len(superset):
            if superset[superset_idx] is not subset[subset_idx]:
                superset_idx += 1
            else:
                found_in_superset = True
                superset_idx += 1
                break
        subset_idx += 1
    return found_in_superset


def does_row_contains_text(row, texts):
    if texts:
        for col in row:
            for text in texts:
                if text in col:
                    return True
    return False


def remove_arr_ele(arr, indices):
    ret_arr = []
    for idx in range(len(arr)):
        if idx not in indices:
            ret_arr.append(arr[idx])
    return ret_arr


def write_to_pdf(file_path, str_content):
    logger.info('Saving PDF %s', file_path)
    with open(file_path, 'wb') as f:
        f.write(str_content)
    return file_path


def delete_file(file_path):
    os.remove(file_path)


def move_file(src_file, target_file):
    try:
        os.rename(src_file, target_file)
    except Exception as e:  # fails for windows system
        logger.debug("Renaming failed: %s. So, deleting target_file and then renaming src_file to target_file" % e)
        delete_file(target_file)
        os.rename(src_file, target_file)


def convert_pdf_to_text(file_path, start_page, end_page=None, encoding='utf-8'):
    """This parses pdf using PDFBOX and returns text as string"""
    return parse_text(file_path, start_page, end_page, encoding)


from pdfminer.pdfpage import PDFPage


def get_total_pages_pdf(file_path):
    count = 0
    with file(file_path, 'rb') as fp:
        for page in PDFPage.get_pages(fp):
            count = count + 1
    return count


def replace_col_with_text_from_alternative_source(csv_file, headers, unique_col, col_to_be_replaced, alternate_data,
                                                  header_included=False, remove_strings_starting_from=[]):
    """This is to update one of columns of a csv from an alternate source.
        If col to be updated is the last one, and `alternate_data` have extra text in next lines,
        you should be providing `remove_strings_starting_from`
        This is implemented assuming unique_col_idx < col_idx
    """

    if unique_col not in headers or col_to_be_replaced not in headers or not alternate_data:
        raise AttributeError("Invalid parameters passed!!")

    unique_col_idx = headers.index(unique_col)
    col_idx = headers.index(col_to_be_replaced)

    if unique_col_idx >= col_idx:  # assumption
        raise AttributeError("Invalid parameters passed!! Column `unique_col` should come before `col_to_be_replaced`")

    dir_name = get_dir_name(csv_file)
    out_file_path = os.path.join(dir_name, "%d.csv" % time.time())  # TODO: use tempfile here

    alternate_data = remove_multi_spaces(
        remove_special_chars(
            alternate_data.replace(TRADEMARK_SYMBOL, TRADEMARK_TEXT)))

    with open(csv_file, 'rb') as in_file, open(out_file_path, 'wb') as out_file:
        writer = csv.writer(out_file)
        reader = csv.reader(in_file)
        if header_included:
            reader.next()
            writer.writerow(headers)

        rows_exhausted = False
        index = 0
        row = reader.next()
        while True:
            if rows_exhausted:
                break
            if len(row) > unique_col_idx and len(row) > col_idx:
                unique_col_val = row[unique_col_idx]
                try:
                    index += find_start_end_index(alternate_data[index:], unique_col_val)[1]
                    text_after_unique_col = remove_multi_spaces(WHITESPACE.join(row[unique_col_idx + 1:col_idx]))
                    text_after_unique_col = text_after_unique_col.strip() or WHITESPACE
                    if text_after_unique_col.strip():
                        index += find_start_end_index(alternate_data[index:], text_after_unique_col)[1]
                    try:
                        next_row = reader.next()
                        if col_idx < len(headers) - 1:
                            next_cols_val = remove_multi_spaces(WHITESPACE.join(row[col_idx + 1:]))
                            next_cols_s_idx = find_start_end_index(alternate_data[index:], next_cols_val)[0]
                            text_to_be_replaced_by = alternate_data[index:index + next_cols_s_idx]
                            index += next_cols_s_idx
                        else:
                            next_cols_val = remove_multi_spaces(WHITESPACE.join(next_row[:unique_col_idx + 1]))
                            next_cols_s_idx = find_start_end_index(alternate_data[index:], next_cols_val)[0]
                            text_to_be_replaced_by = alternate_data[index:index + next_cols_s_idx]
                            index += next_cols_s_idx
                    except StopIteration:
                        text_to_be_replaced_by = alternate_data[index:]
                        rows_exhausted = True

                    for text in remove_strings_starting_from:
                        if text in text_to_be_replaced_by:
                            text_to_be_replaced_by = text_to_be_replaced_by[
                                                     :find_start_end_index(text_to_be_replaced_by, text)[0]]

                    row[col_idx] = text_to_be_replaced_by.strip()

                except Exception as e:
                    print e, "for", unique_col_val
                    raise
            writer.writerow(row)
            row = next_row

    move_file(out_file_path, csv_file)
    return csv_file


def find_start_end_index(search_str, sub_str):
    sub_str = remove_multi_spaces(sub_str)
    start_index = search_str.index(sub_str)
    end_index = start_index + len(sub_str)
    return start_index, end_index


def remove_multi_spaces(text, replace_by=WHITESPACE):
    if DOUBLESPACE in text:
        return remove_multi_spaces(text.replace(DOUBLESPACE, replace_by))
    else:
        return text


def num_of_occurrences(string, sub):
    count = start = 0
    while True:
        start = string.find(sub, start) + 1
        if start > 0:
            count += 1
        else:
            return count
