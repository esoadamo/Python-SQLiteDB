from pathlib import Path
from random import choice
from typing import Union, Optional, Callable, Dict, Any

from .sqlitedb import SQLiteDB, is_main_thread_running


class IndexedDBManager:
    def __init__(self,
                 file: Union[Path, str],
                 autoquit_test: Optional[Callable[[], bool]] = is_main_thread_running
                 ) -> None:
        self.sqlitedb = SQLiteDB(file, autoquit_test)
        self.__db_cache: Dict[str, "IndexedDB"] = {}

    def __getitem__(self, item: str) -> "IndexedDB":
        if len(self.__db_cache) > 128:
            del self.__db_cache[choice(list(self.__db_cache.keys()))]
        if item not in self.__db_cache:
            self.__db_cache[item] = IndexedDB(self, item)
        return self.__db_cache[item]


class IndexedDB:
    def __init__(self, manager: IndexedDBManager, db_name: str):
        self.__manager = manager
        self.__table = "`db_%s`" % db_name.replace('`', '``')
        manager.sqlitedb.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.__table} (
        `id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        `key`	TEXT NOT NULL UNIQUE,
        `value`	TEXT
        );
        """)

    def get(self, item: str, default: Optional[Any] = None) -> Any:
        try:
            return self[item]
        except IndexError:
            return default

    def key_exists(self, item: str) -> bool:
        return not not self.__manager.sqlitedb.execute(f"SELECT value FROM {self.__table} WHERE key=?", (item,))

    def __getitem__(self, item: str) -> Any:
        r = self.__manager.sqlitedb.execute(f"SELECT value FROM {self.__table} WHERE key=?", (item,))
        if r:
            return r[0][0]
        raise IndexError

    def __setitem__(self, key: str, value: Any) -> None:
        if self.key_exists(key):
            self.__manager.sqlitedb.execute(f"UPDATE {self.__table} SET value=? WHERE key=?", (value, key))
        else:
            self.__manager.sqlitedb.execute(
                f"INSERT INTO {self.__table} (`key`,`value`) VALUES (?,?);", (key, value)
            )
        self.__manager.sqlitedb.commit()

    def __delitem__(self, key: str) -> None:
        self.__manager.sqlitedb.execute(
            f"DELETE FROM {self.__table} WHERE `key`=?;", (key,)
        )
