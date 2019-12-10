from dependency_injector import providers, containers
from logger import DataStoreLogger

class Dependencies(containers.DeclarativeContainer):
    log = providers.Singleton(DataStoreLogger)