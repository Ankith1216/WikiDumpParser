from XMLExtractor import XMLExtractor
from bz2 import BZ2File
import os
from queue import Queue
from threading import Lock, Thread
import re
from collections import Counter
from nltk.corpus import stopwords
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager, Process

cachedStopWords = stopwords.words('english')

avoid_characters = ('[', '*', '-', '|', '=', '<', '{', '!', '}', '#', ':', ';')
redirect_pattern = re.compile("#REDIRECT", re.IGNORECASE)

def parser_worker(input_queue_, output_queue_, identifier_):
    while True:
        item = input_queue_.get()
        if item == None:
            input_queue_.put(item)
            break

        page = item[1]
        # print(f'>parser worker {identifier_} received work')
        if not bool(redirect_pattern.match(page)):
            for line in page.split("\n"):
                line = line.strip()
                if line and line[0] not in avoid_characters:
                    output_queue_.put((line, False))
            output_queue_.put(("", True)) 

        # output_queue_.put(item)

def file_writer(input_queue_, identifier_):
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
    queue_manager = Manager()
    input_queue = queue_manager.Queue(maxsize=2000)
    output_queue = queue_manager.Queue(maxsize=2000)

    processes = []
    for i in range(4):
        p= Process(target=parser_worker, args=(input_queue, output_queue, i))
        p.start()
        processes.append(p)

    writer_pool = ThreadPoolExecutor(max_workers=4)
    for i in range(4):
        writer_pool.submit(file_writer, output_queue, i)

    wiki_file_obj = BZ2File(os.path.join(os.path.dirname(os.path.realpath(__file__)), './enwiki-20220701-pages-articles-multistream1.xml-p1p41242.bz2'))
    extractor = XMLExtractor(wiki_file_obj, input_queue)
    extractor.extract()

    input_queue.put(None)
    for p in process:
        p.join()

    output_queue.put(None)
    writer_pool.shutdown()

