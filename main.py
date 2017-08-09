import statistics
import re
import time
import PyPDF2
import urllib.error
import urllib.request
import os
import logging
import json
import matplotlib.pyplot as plt
import unidecode
import collections
import multiprocessing

configuration_filename = 'configuration.json'


class LoggedTask():
    def __init__(self, logger, name):
        self.logger = logger
        self.start = time.time()
        self.name = name
        logger.info('Started {}.'.format(name))

    def finish(self):
        delta = time.time() - self.start
        logger.info('Finished {} after {:.3f} s.'.format(self.name, delta))


class Aggregate():
    def __init__(self, name: str):
        self.name = name
        self.data = {}

    def update_data(self, indicator: str, period: str, value: float):
        if indicator not in self.data:
            self.data.update({indicator: collections.OrderedDict()})
        if period in self.data[indicator]:
            raise ValueError('{} already in {}!'.format(period, indicator))
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

    def plot(self, path):
        for indicator in self.data:
            figure = plt.figure()
            plt.title(self.name + ': ' + indicator)
            plot_path = os.path.join(path, normalize_name(self.name))
            full_plot_path = os.path.join(plot_path, normalize_name(indicator) + '.svg')
            ensure_path_exists(plot_path)
            figure.savefig(full_plot_path)
            plt.close()


def ensure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == os.errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def download(target_tuple):
    url = target_tuple[0]
    path = target_tuple[1]
    if os.path.exists(path):
        return
    try:
        urllib.request.urlretrieve(url, path)
    except urllib.error.HTTPError:
        pass
    except:
        logger.error('Unhandled exception!')
        raise


def get_worker_count():
    return multiprocessing.cpu_count()


def initialize_sources(logger):
    with open(configuration_filename) as configuration_handler:
        configuration = json.load(configuration_handler)
    base_path = configuration['sources_base_path']
    root = configuration['sources_root']
    ensure_path_exists(root)
    logger.info('Started downloading data files.')
    # Download all first 20 XX.pdf, failing silently on 404s.
    arguments = []
    for i in range(1, 20):
        filename = '{:02}.pdf'.format(i)
        source = base_path + filename
        destination = os.path.join(root, filename)
        arguments.append((source, destination))
    start = time.time()
    with multiprocessing.Pool(get_worker_count()) as pool:
        pool.map(download, arguments)
    end = time.time()
    delta = end - start
    logger.info('Finished downloading data files after {:.3f} s.'.format(delta))


def extract_text(pdf_filename):
    with open(pdf_filename, 'rb') as file_handle:
        reader = PyPDF2.PdfFileReader(file_handle)
        contents = []
        for page in range(reader.getNumPages()):
            contents.extend(reader.getPage(page).extractText().split('\n'))
        # Return non-empty only.
        return [string for string in contents if string]


def normalize_name(name):
    name = name.lower()
    name = unidecode.unidecode(name)
    name = name.replace(' ', '-')
    while '--' in name:
        name = name.replace('--', '-')
    name = name.replace('(', '')
    name = name.replace(')', '')
    return name


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
    logger.info('Using {} process{}.'.format(get_worker_count(), '' if get_worker_count() == 1 else 'es'))
    initialize_sources(logger)
    with open(configuration_filename) as configuration_handler:
        configuration = json.load(configuration_handler)
    sources_root = configuration['sources_root']

    task = LoggedTask(logger, 'extracting text')
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
        while i < len(strings):
            # New page, skip headers.
            if strings[i] == 'Universidade Federal do Rio Grande do Sul':
                i += 4 + len(periods)
                continue
            if strings[i][0].isdigit():
                # Some of the data ends up being malformed (great job, UFRGS). So we skip these lines.
                i += len(periods)
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
    task.finish()

    task = LoggedTask(logger, 'dumping aggregates')
    with open('aggregate.txt', 'w') as aggregate_handle:
        for aggregate in aggregates.values():
            aggregate_handle.write(str(aggregate))
    task.finish()

    if configuration['plot_everything']:
        task = LoggedTask(logger, 'plotting graphs')
        for aggregate in aggregates.values():
            aggregate.plot(configuration['plots_root'])
        task.finish()

    # Plot the combination of the average and the computer science data.
    task = LoggedTask(logger, 'making report')
    totals = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
    counts = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
    for aggregate in aggregates.values():
        for indicator in aggregate.data:
            for period in aggregate.data[indicator]:
                if aggregate.data[indicator][period]:
                    totals[indicator][period] += aggregate.data[indicator][period]
                    counts[indicator][period] += 1
    average = Aggregate('Média')
    for indicator in totals:
        for period in totals[indicator]:
            average.update_data(indicator, period, totals[indicator][period] / counts[indicator][period])
    computer_science = None
    for aggregate in aggregates.values():
        if aggregate.name.startswith('Ciência da Computação'):
            computer_science = aggregate
            break
    # TODO: compute minimum and maximum.
    markdown_code = []
    markdown_code.append('# UFRGS Evaluation Aggregator')
    markdown_code.append('\n')
    markdown_code.append(configuration['project_description'])
    markdown_code.append('\n')
    markdown_code.append('\n')
    for indicator in sorted(average.data):
        plt.title('Comparação' + ': ' + indicator)
        padding = 0.4
        plt.axis([-padding, len(average.data[indicator].values()) - 1 + padding, 3 - padding / 2, 5 + padding / 2])
        # Find the best and worst at this indicator.
        min_name = ''
        min_values = [5.0]
        max_name = ''
        max_values = [0.0]
        for aggregate in aggregates.values():
            if not aggregate.data[indicator]:
                continue
            values = [value for value in aggregate.data[indicator].values() if value is not None]
            if not values:
                continue
            if statistics.median(values) < statistics.median(min_values):
                min_values = values
                min_name = aggregate.name
            if statistics.median(values) > statistics.median(max_values):
                max_values = values
                max_name = aggregate.name
        labels = []
        average_values = []
        computer_science_values = []
        for period in sorted(average.data[indicator]):
            labels.append(period)
            average_values.append(average.data[indicator][period])
            computer_science_values.append(computer_science.data[indicator][period])
        plt.plot(average_values, label='Média')
        plt.plot(computer_science_values, label='Ciência da Computação')
        # Because minimum and maximum may have missing initial values, we have to supply X too.
        min_x = range(len(computer_science_values) - len(min_values), len(computer_science_values))
        max_x = range(len(computer_science_values) - len(max_values), len(computer_science_values))
        plt.plot(min_x, min_values, label=min_name[:min_name.find('(')].strip())
        plt.plot(max_x, max_values, label=max_name[:max_name.find('(')].strip())
        plt.xticks(range(len(labels)), labels)
        plot_path = configuration['reports_root']
        plot_filename = normalize_name(indicator) + '.png'
        full_plot_path = os.path.join(plot_path, plot_filename)
        ensure_path_exists(plot_path)
        plt.margins(0.5)
        axis = plt.subplot(111)
        box = axis.get_position()
        axis.set_position([box.x0, box.y0 + box.height * 0.25, box.width, box.height * 0.75])
        plt.legend(bbox_to_anchor=(0.5, -0.05), loc='upper center')
        plt.savefig(full_plot_path)
        plt.close()
        plot_url = configuration['reports_root'] + '/' + plot_filename
        markdown_code.append('![]({})'.format(plot_url))
        markdown_code.append('\n')
        markdown_code.append('\n')
    task.finish()
    with open('README.md', 'w') as readme:
        readme.writelines(markdown_code)
