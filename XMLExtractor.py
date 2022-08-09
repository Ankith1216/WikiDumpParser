import xml.etree.ElementTree as ET

class XMLExtractor:
    def __init__(self, file_path, queue):
        self._file_path = file_path
        self._page = None
        self._tag_stack = None
        self._queue = queue
        self._cnt = 0

    def extract(self):
        for event, element in ET.iterparse(self._file_path, events=('start', 'end')):
            tag_name = element.tag.rsplit('}', 1)[-1].strip()

            if event == 'start':
                if tag_name == 'page':
                    self._page = {'title':"", 'text':""}
                    self._tag_stack = []

                if self._page is not None:
                    self._tag_stack.append(tag_name)
            else:
                if self._page is not None:
                    if self._tag_stack[-1] == 'title':
                        self._page['title'] += element.text
                    elif self._tag_stack[-1] == 'text':
                        self._page['text'] += element.text
                    
                    if self._tag_stack[-1] == 'page':
                        self._cnt += 1
                        self._queue.put((self._page['title'], self._page['text'], self._cnt))
                        self._page = None
                        self._tag_stack = None
                    else:
                        del self._tag_stack[-1]

                element.clear()
