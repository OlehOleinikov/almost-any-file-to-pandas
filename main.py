"""
Multi-format parser for pandas DataFrame

Main class FileToPandasImporter (pattern Strategy without context change)
1. Receive file path with ".parse(path: str)" method
2. Verify file format, permission and select the parser
3. Determine the number of sheets (for table or tree-like files xml, xlsx, xlsx)
4. Verify encoding and delimiter for text files
5. Convert file to pandas DataFrame

The .parse(path: str) method returns a list of ParserAnswer instances, with attributes:
      sheet_name: str
      data: pd.DataFrame
      encoding: str
      separator: str
      engine: str
      file_path: str
      parse_info: str
"""
from abc import abstractmethod, ABC
from typing import Union, List

import pandas as pd
from lxml import etree as et
from tabula import read_pdf
import csv

from charset_normalizer import detect
from pathlib import Path
from loguru import logger


class ParserAnswerDescriptor(ABC):
    """Abstract descriptor for generating descriptors with validation of attributes"""
    @classmethod
    @abstractmethod
    def verify(cls, value):
        pass

    def __set_name__(self, owner, name):
        self.name = "_" + name

    def __get__(self, instance, owner):
        return getattr(instance, self.name)

    def __set__(self, instance, value):
        value = self.verify(value)
        setattr(instance, self.name, value)


class RulePath(ParserAnswerDescriptor):
    """Descriptor for file path attr, convert to asb path string"""
    @classmethod
    def verify(cls, value):
        if not isinstance(value, Path):
            value = Path(value)
        return str(value.absolute())


class RuleString(ParserAnswerDescriptor):
    """Descriptor for string like attrs"""
    @classmethod
    def verify(cls, value):
        if not isinstance(value, str):
            value = str(value)
        return value


class RuleData(ParserAnswerDescriptor):
    """Descriptor for main data attr, if not - create empty pd.DataFrame"""
    @classmethod
    def verify(cls, value):
        if not isinstance(value, pd.DataFrame):
            value = pd.DataFrame()
        return value


class ParserAnswer:
    """
    Class of saving the results of parsing the page of the file (pandas dataframe and related information).
    To get results in pd.DataFrame format - call the ".data" attribute
    """
    __slots__ = ["_sheet_name", "_data", "_encoding", "_separator", "_engine", "_file_path", "_parse_info"]
    file_path = RulePath()
    sheet_name = RuleString()
    data = RuleData()
    encoding = RuleString()
    separator = RuleString()
    engine = RuleString()
    parse_info = RuleString()

    def __init__(self, **kwargs):
        unexpected_keys = set(kwargs.keys()) - set([x[1:] for x in self.__slots__])
        if unexpected_keys:
            raise TypeError(f'Unexpected key(s): {unexpected_keys} for {self.__class__.__name__} class')

        self.file_path = kwargs['file_path']
        self.sheet_name = kwargs.get('sheet_name')
        self.engine = kwargs.get('engine', 'Not used')
        self.encoding = kwargs.get('encoding', 'not applied')
        self.separator = kwargs.get('separator', 'format defined')
        self.data = kwargs.get('data')
        self.parse_info = 'Failed' if self.data.shape[0] == 0 else 'OK'

    def __str__(self):
        return f'Parse result for: {self.file_path} (sheet name: {self.sheet_name}) ' \
               f'\n\tUsed engine:    {self.engine}' \
               f'\n\tEncoding:       {self.encoding}' \
               f'\n\tText separator: {self.separator}' \
               f'\n\tParsed columns: {self.data.shape[1]}' \
               f'\n\tParsed rows:    {self.data.shape[0]}' \
               f'\n\tStatus:         {self.parse_info}'

    def __repr__(self):
        return self.__str__()


class FileToPandasImporter:
    """
    Extract tabular data from the file. Checking multiple sheets for * .xls(x), * .xml.
    Export to pandas.DataFrame format with distribution on detected sheets and additional information.
    """
    @staticmethod
    def parse(path: Union[str, Path]):
        """
        Choose importer, call importer's work method

        :return: list of results (each list corresponds to one sheet)
        :rtype: List[ParserAnswer]
        """
        file_path = Path(path)
        extension = file_path.suffix.lower()

        # Check file (present, permission)
        try:
            assert file_path.exists()
            with open(path, 'rb'):
                pass
        except AssertionError:
            logger.error(f"File '{file_path}' not found. Possibly moved or deleted")
            return [ParserAnswer(file_path=file_path)]
        except PermissionError:
            logger.error(f"File '{file_path}' blocked. Possibly use by another app")
            return [ParserAnswer(file_path=file_path)]

        # Choose a parser
        if extension in [".xlsx", ".xls", ".xlsb", ".odf", ".ods", ".odt"]:
            parser = ImportExcel(file_path=file_path)
        elif extension == ".xml":
            parser = ImportXML(file_path=file_path)
        elif extension in [".txt", ".csv", ".ini"]:
            parser = ImportText(file_path=file_path)
        elif extension == ".ant":
            parser = ImportText(file_path=file_path, delimiter='~~@~~')
        elif extension == ".pdf":
            parser = ImportPDF(file_path=file_path, concat=True)
        elif extension == ".parquet":
            parser = ImportParquet(file_path=file_path)
        elif extension == ".json":
            parser = ImportJSON(file_path=file_path)
        elif extension in [".pk1", "pickle"]:
            parser = ImportPickle(file_path=file_path)
        else:
            logger.warning(f"Unknown format '{file_path}', extract failed")
            return [ParserAnswer(file_path=file_path)]

        # Extract data
        return parser.work()


class AbstractImporter(ABC):
    """
    Abstract class, describes the formation of parser classes for a specific file format
    """

    def __init__(self, file_path):
        self.file_path = Path(file_path)

    @abstractmethod
    def work(self, *args, **kwargs):
        """
        Loads pure data without pre - processing (except encoding checks, delimiter definition in text files)

        :return: List[ParserAnswer]
        :rtype: List[ParserAnswer]
        """
        pass

    def get_encoding(self) -> Union[str, None]:
        """Define file encoding (for plain text files)"""
        default_encoding = None
        try:
            with open(self.file_path, 'rb') as file:
                file_info = detect(file.read())
                encoding = file_info['encoding']
                return encoding
        except Exception as err:
            logger.warning(f'Encoding check failed in {self.file_path.name}: "{err}" (set by default).')
            return default_encoding

    def get_text_delimiter(self) -> str:
        """Determines the delimiters of the text from a random sample of lines, voting determines the most likely"""
        path = self.file_path
        default_delimiter = '\t'
        delimiters_to_delete = [' ', ","]
        number_of_samples = 15
        with open(path, 'r') as file:
            line_count = file.read().count("\n")
            positions_to_test = []
            current_pos = 0
            step = int(line_count / number_of_samples)
            for test_number in range(number_of_samples):
                positions_to_test.append(current_pos + step * test_number)

            detected_delimiters_list = []
            for position in positions_to_test:
                try:
                    file.seek(0)
                    content = file.readlines()
                    sample = content[position]
                    for delimiter in delimiters_to_delete:
                        sample = content[position].replace(delimiter, '')
                    dialect = csv.Sniffer().sniff(sample)
                    delimiter_detected = str(dialect.delimiter)
                    detected_delimiters_list.append(delimiter_detected)
                except Exception as sniffer_error:
                    logger.warning(f'Delimiter check failed "{sniffer_error}" in {path.name} (position "{position}")')
                    logger.warning(f'Delimiter set by default (tab symbol) in {path.name}')
                    return default_delimiter

        if len(detected_delimiters_list) == 0:
            logger.warning(f'Delimiter not found in {path.name} (set by default (tab symbol))')
            return default_delimiter
        else:
            delimiter_detected = max(set(detected_delimiters_list), key=detected_delimiters_list.count)
            return delimiter_detected


class ImportExcel(AbstractImporter):
    """
    Table file parser (".xlsx", ".xls", "xlsb", ".odf", ".ods", ".odt").
    """

    def __init__(self, file_path):
        super().__init__(file_path)

    def work(self):
        result = []
        file_connect = pd.ExcelFile(self.file_path)
        sheets = file_connect.sheet_names
        if not sheets:
            logger.error(f'Can\'nt find sheets of Excel-like file: {self.file_path.name}')
            return [ParserAnswer(file_path=self.file_path.absolute(), engine=self.__class__.__name__)]
        for sheet in file_connect.sheet_names:
            df = pd.read_excel(self.file_path,
                               sheet_name=sheet,
                               header=None,
                               index_col=None,
                               dtype=str)
            result.append(ParserAnswer(sheet_name=sheet,
                                       data=df,
                                       engine=self.__class__.__name__,
                                       file_path=self.file_path.absolute(),
                                       parse_info="OK"))
        return result


class ImportXML(AbstractImporter):
    """Tree-like XML file parser, MicroSoft namespame used"""
    def __init__(self, file_path):
        super().__init__(file_path)

    def work(self):
        result = []
        schema = './/{urn:schemas-microsoft-com:office:spreadsheet}'
        parser = et.XMLParser(recover=True, huge_tree=True)
        tree = et.parse(str(self.file_path), parser)
        root = tree.getroot()

        sheets = root.findall(schema + 'Worksheet')
        if len(sheets) > 0:  # case if sheets divide present
            for sheet in sheets:
                sheet_name = sheet.attrib.get(schema[3:] + 'Name')
                tables = sheet.findall(schema + 'Table')
                for table in tables:
                    df = self.parse_table_section(table, schema)
                    result.append(ParserAnswer(sheet_name=sheet_name,
                                               data=df,
                                               engine=self.__class__.__name__,
                                               file_path=self.file_path.absolute(),
                                               parse_info="OK"))
            return result
        else:  # case only table present
            logger.warning(f'Can\'t find worksheet keywords in XML file: {self.file_path.name}')
            tables = root.findall(schema + 'Table')
            if len(tables) > 0:
                for table in tables:
                    df = self.parse_table_section(table, schema)
                    result.append(ParserAnswer(sheet_name="Not defined",
                                               data=df,
                                               engine=self.__class__.__name__,
                                               file_path=self.file_path.absolute(),
                                               parse_info="OK"))
                return result
            else:  # case no nodes found
                logger.error(f'Can\'t find root in XML file: {self.file_path.name}. Check XML schema and keywords')
                return [ParserAnswer(file_path=self.file_path.absolute(), engine=self.__class__.__name__)]

    @staticmethod
    def parse_table_section(table, schema):
        """Helper func for combine dataframe from XML section"""
        data_dict = {}
        row_num = 0
        rows = table.findall(schema + 'Row')
        for row in rows:
            data_pointers = row.findall(schema + 'Data')
            data = []
            for point in data_pointers:
                data.append(point.text)
            if len(data) > 0:
                data_cur_dict = {row_num: data}
                data_dict.update(data_cur_dict)
                row_num += 1
        return pd.DataFrame.from_dict(data_dict, orient='index', dtype=str)


class ImportText(AbstractImporter):
    """Plain text file importer"""
    def __init__(self, file_path, delimiter=None):
        self.delimiter = delimiter
        super().__init__(file_path)

    def work(self):
        detected_encoding = self.get_encoding()  # check encoding
        if not self.delimiter:
            self.delimiter = self.get_text_delimiter()  # get delimiter if not present in args
        largest_column_size = self.max_cols_in_rows()  # get columns shape

        with open(self.file_path, 'r') as file:
            current_index = 0
            data_dict = {}
            file.seek(0)
            lines = file.readlines()
            for line in lines:  # compare dict for dataframe creation
                line = line.strip('\n').strip('\t')
                row = [''] * largest_column_size
                for pos, data in enumerate(line.split(self.delimiter)):
                    row[pos] = data.strip('\"').strip('\'')
                data_dict.update({current_index: row})
                current_index += 1
            df = pd.DataFrame.from_dict(data_dict, orient='index', dtype=str)
            return [ParserAnswer(sheet_name="Text file content",
                                 data=df,
                                 encoding=detected_encoding,
                                 separator=self.delimiter,
                                 engine=self.__class__.__name__,
                                 file_path=self.file_path.absolute(),
                                 parse_info="OK")]

    def max_cols_in_rows(self):
        largest_column_size = 0
        with open(self.file_path, 'r') as file:
            lines = file.readlines()
            for line in lines:  # get max columns count for each row
                line = line.strip('\n').strip('\t')
                column_size = len(line.split(self.delimiter))
                largest_column_size = column_size if largest_column_size < column_size else largest_column_size
        return largest_column_size


class ImportPDF(AbstractImporter):
    """PDF importer for normal structure PDF file. In any case need to recheck dataframe"""
    def __init__(self, file_path, concat=True):
        self.concat = concat
        super().__init__(file_path)

    def work(self):
        logger.warning('PDF download can be long (2-4 pages / sec). The structure may be damaged')
        result = []
        path = self.file_path
        df_list = read_pdf(path, pandas_options={'header': None}, pages="all")
        valid_df = pd.DataFrame()
        invalid_df = pd.DataFrame()
        first_page_cols = df_list[0].shape[1]
        if self.concat:
            for pos, tab in enumerate(df_list):
                if df_list[pos].shape[1] == first_page_cols:
                    valid_df = pd.concat([valid_df, df_list[pos]], ignore_index=True, sort=False)
                else:
                    invalid_df = pd.concat([invalid_df, df_list[pos]], ignore_index=True, sort=False)
            valid_df.reset_index(inplace=True)
            invalid_df.reset_index(inplace=True)
            result.append(ParserAnswer(sheet_name="PDF file content (concated)",
                                       data=valid_df,
                                       engine=self.__class__.__name__,
                                       file_path=self.file_path.absolute(),
                                       parse_info="OK"))
            if invalid_df.shape[0] > 0:
                result.append(ParserAnswer(sheet_name="PDF file content (unsized)",
                                           data=invalid_df,
                                           engine=self.__class__.__name__,
                                           file_path=self.file_path.absolute(),
                                           parse_info="OK"))
            return result
        else:
            for tab in df_list:
                result.append(ParserAnswer(sheet_name="PDF file content (by page)",
                                           data=tab,
                                           engine=self.__class__.__name__,
                                           file_path=self.file_path.absolute(),
                                           parse_info="OK"))
            return result


class ImportParquet(AbstractImporter):
    def __init__(self, file_path):
        super().__init__(file_path)

    def work(self):
        df = pd.read_parquet(self.file_path)
        return [ParserAnswer(sheet_name="Parquet file content",
                             data=df,
                             engine=self.__class__.__name__,
                             file_path=self.file_path.absolute(),
                             parse_info="OK")]


class ImportJSON(AbstractImporter):
    def __init__(self, file_path):
        super().__init__(file_path)

    def work(self):
        df = pd.read_json(self.file_path)
        return [ParserAnswer(sheet_name="JSON file content",
                             data=df,
                             engine=self.__class__.__name__,
                             file_path=self.file_path.absolute(),
                             parse_info="OK")]


class ImportPickle(AbstractImporter):
    def __init__(self, file_path):
        super().__init__(file_path)

    def work(self):
        df = pd.read_pickle(self.file_path)
        return [ParserAnswer(sheet_name="JSON file content",
                             data=df,
                             engine=self.__class__.__name__,
                             file_path=self.file_path.absolute(),
                             parse_info="OK")]


if __name__ == '__main__':
    parse_result = FileToPandasImporter().parse('example.txt')
    print('Parse result instance:\n', parse_result)
    print('Parse result type: ', type(parse_result))
    print('Parse result element type: ', type(parse_result[0]))
    print('Parse result data attr type: ', type(parse_result[0].data))
    print('\nParse result dataframe:')
    print(parse_result[0].data, end='\n\n\n')

    multi_pages_example = FileToPandasImporter().parse('example.xlsx')
    print(f"Pages list for '{Path(multi_pages_example[0].file_path).name}'")
    for order, page in enumerate(multi_pages_example):
        print(f'\t{order+1} page name is "{page.sheet_name}"')
