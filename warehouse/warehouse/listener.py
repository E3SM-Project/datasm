from time import sleep
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from warehouse.util import setup_logging, log_message


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
        self.my_event_handler = RegexMatchingEventHandler(
            patterns, ignore_patterns, ignore_directories, case_sensitive)

        self.my_event_handler.on_created = self.on_created
        self.my_event_handler.on_modified = self.on_modified
        setup_logging('debug', f'listener.log')

    def start(self):
        # print("Starting up filesystem listener")
        log_message('info',"Starting up filesystem listener")
        self.observer = Observer()
        self.observer.schedule(
            self.my_event_handler,
            self.root, 
            recursive=False)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()

    def on_created(self, event):
        log_message('info',f"{event.src_path} has been created")
        # print(f"{event.src_path} has been created")

    def on_modified(self, event):
        self.warehouse.status_was_updated(event.src_path)


if __name__ == "__main__":
    listener = Listener(warehouse=None)
    listener()
