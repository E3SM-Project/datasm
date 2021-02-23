from time import sleep
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler


class Listener(object):

    def __init__(self, warehouse, root, **kwargs):
        super().__init__(**kwargs)
        self.warehouse = warehouse
        self.root = root
        self.observer = None

        patterns = [r'^.*\/\.status$']
        ignore_patterns = ""
        ignore_directories = True
        case_sensitive = True
        self.my_event_handler = RegexMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

        self.my_event_handler.on_created = self.on_created
        self.my_event_handler.on_modified = self.on_modified

    def start(self):
        print("Starting up filesystem listener")
        go_recursively = True
        self.observer = Observer()
        self.observer.schedule(self.my_event_handler, self.root, recursive=go_recursively)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()

    def on_created(self, event):
        print(f"{event.src_path} has been created")

    def on_modified(self, event):
        with open(event.src_path, 'r') as instream:
            lines = [line for line in instream.readlines() if 'STAT' in line]
        if 'Engaged' not in lines[-1]:
            self.warehouse.status_was_updated(event.src_path)


if __name__ == "__main__":
    listener = Listener(warehouse=None)
    listener()