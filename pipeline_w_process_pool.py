from XMLExtractor import XMLExtractor
from bz2 import BZ2File
import os
from queue import Queue
from threading import Lock, Thread
import re
from collections import Counter
from nltk.corpus import stopwords
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager

cachedStopWords = stopwords.words('english')

def parse_string(str_):
    str_ = str_.lower()
    # str_ = re.sub('[\W_]+', " ", str_).strip()
    return str_

def parser_worker(input_queue_, identifier_):
    f = open(f'files/out_{identifier_}.txt', 'w')
    while True:
        item = input_queue_.get()
        if item == None:
            input_queue_.put(item)
            break
        if((item[2]%10000) == 0):
            print(input_queue_.qsize(), output_queue_.qsize())
            print('Parser: ', item[2])
        item = item[1]
        # print(f'>parser worker {identifier_} received work')
        item = parse_string(item)
        f.write(item)
        # output_queue_.put(item)
    f.close()

def counter_worker(input_queue_, identifier_):
    f = open(f'files/out_{identifier_}.txt', 'w')
    while True:
        item = input_queue_.get()
        if item == None:
            input_queue_.put(item)
            break
        # print(f'>counter worker {identifier_} received work')
        
        f.write(item)

    f.close()


if __name__ == '__main__':
    # input_queue, output_queue = Queue(maxsize=2000), Queue(maxsize=2000)
    queue_manager = Manager()
    input_queue = queue_manager.Queue(maxsize=30000)
    # output_queue = queue_manager.Queue(maxsize=2000)


    wiki_file_obj = BZ2File(os.path.join(os.path.dirname(os.path.realpath(__file__)), './enwiki-20220701-pages-articles-multistream1.xml-p1p41242.bz2'))
    
    extractor = XMLExtractor(wiki_file_obj, input_queue)

    parser_pool = ProcessPoolExecutor(max_workers=4)
    # counter_pool = ProcessPoolExecutor(max_workers=4)
    for i in range(4):
        parser_pool.submit(parser_worker, input_queue, i)

    # for i in range(4):
        # counter_pool.submit(counter_worker, output_queue, i)

    """
    parser_workers = [Thread(target=parser_worker, args=(input_queue, output_queue, i)) for i in range(3)]
    counter_workers = [Thread(target=counter_worker , args=(output_queue, i)) for i in range(3)]

    for worker in parser_workers:
        worker.start()

    for worker in counter_workers:
        worker.start()
    """

    extractor.extract()

    input_queue.put(None)
    # for worker in parser_workers:
        # worker.join()
    parser_pool.shutdown()

    # output_queue.put(None)
    # for worker in counter_workers:
        # worker.join()
    # counter_pool.shutdown()


