import re
import PyPDF2
import urllib.error
import urllib.request
import os
import sys
import logging
import json
import matplotlib.pyplot as plt

configuration_filename = 'configuration.json'


class Aggregate():
    def __init__(self, name: str):
        self.name = name
        self.data = {}

    def update_data(self, indicator: str, period: str, value: float):
        if indicator not in self.data:
            self.data.update({indicator: {}})
        if period in self.data[indicator]:
            raise ValueError
        self.data[indicator].update({period: value})

    def __str__(self):
        bits = []
        bits.append(self.name)
        bits.append('\n')
        for indicator in self.data:
            bits.append(' ' * 2)
            bits.append(indicator)
            bits.append('\n')
            for value in sorted(self.data[indicator]):
                bits.append(' ' * 4)
                bits.append(value)
                bits.append(' ')
                bits.append(str(self.data[indicator][value]))
                bits.append('\n')
        return ''.join(bits)


def ensure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == os.errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def initialize_sources(logger):
    with open(configuration_filename) as configuration_handler:
        configuration = json.load(configuration_handler)
    base_path = configuration['sources_base_path']
    root = configuration['sources_root']
    ensure_path_exists(root)
    logger.info('Started downloading data files...')
    # Download all XX.pdf until we get a 404.
    for i in range(1, 100):
        filename = '{:02}.pdf'.format(i)
        full_url = base_path + filename
        source_path = os.path.join(root, filename)
        if os.path.exists(source_path):
            continue
        try:
            urllib.request.urlretrieve(full_url, source_path)
        except urllib.error.HTTPError:
            downloaded = i - 1
            logger.info('Stopped after downloading {} file{}.'.format(downloaded, '' if downloaded == 1 else 's'))
            break
        except:
            logger.error('Unhandled exception!')
            raise
    logger.info('Finished downloading data files.')


def extract_text(pdf_filename):
    with open(pdf_filename, 'rb') as file_handle:
        reader = PyPDF2.PdfFileReader(file_handle)
        contents = reader.getPage(0).extractText().split('\n')
        return contents


def normalize_name(name):
    return name.lower().replace(' ', '-')


def make_logger(filename):
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    return logger


if __name__ == '__main__':
    logger = make_logger('log.txt')
    initialize_sources(logger)
    with open(configuration_filename) as configuration_handler:
        configuration = json.load(configuration_handler)
    sources_root = configuration['sources_root']
    # A map from course name to aggregate object.
    aggregates = {}
    for pdf_file in os.listdir(sources_root):
        strings = extract_text(os.path.join(sources_root, pdf_file))
        indicator = ' '.join(strings[2].split()[1:])
        # The number of periods is variable, so we have to count them.
        periods = []
        i = 4
        period_re = re.compile('\d{4}/\d')
        while period_re.match(strings[i]):
            periods.append(strings[i])
            i += 1
        # From this point onwards we will match the course name and the evaluations.
        # There may be empty strings at the end, so we check against those.
        while i < len(strings) and strings[i]:
            name = strings[i]
            i += 1
            if name not in aggregates:
                aggregates.update({name: Aggregate(name)})
            for period in periods:
                try:
                    value = float(strings[i].replace(',', '.'))
                except ValueError:
                    value = None
                aggregates.get(name).update_data(indicator, period, value)
                i += 1

    with open('aggregate.txt', 'w') as aggregate_handle:
        for aggregate in aggregates.values():
            print(str(aggregate), file=aggregate_handle)
