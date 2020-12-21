import json
from pathlib import Path
from random import choice
from typing import Union, Optional, Dict, Any, Set, Iterator, Tuple

from .sqlitedb import SQLiteDB


class IndexedDBManager:
    def __init__(self, file: Union[Path, str]) -> None:
        self.sqlitedb = SQLiteDB(file)
        self.__db_cache: Dict[str, "IndexedDB"] = {}

    def keys(self) -> Set[str]:
        tables = self.sqlitedb.execute("SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name LIKE 'db_%'")
        return {table[0][3:] for table in tables}

    def values(self) -> Iterator["IndexedDB"]:
        return map(lambda x: x[1], self.items())

    def items(self) -> Iterator[Tuple[str, "IndexedDB"]]:
        for k in self.keys():
            yield k, self[k]

    def __str__(self):
        return str({k: str(v) for k, v in self.items()})

    def __getitem__(self, item: str) -> "IndexedDB":
        if len(self.__db_cache) > 128:
            del self.__db_cache[choice(list(self.__db_cache.keys()))]
        if item not in self.__db_cache:
            self.__db_cache[item] = IndexedDB(self, item)
        return self.__db_cache[item]

    def __delitem__(self, key: str) -> None:
        self[key].drop_db()

    def __contains__(self, item: str) -> bool:
        return item in self.keys()

    def __del__(self) -> None:
        self.sqlitedb.quit()

    def __bool__(self) -> bool:
        return not not self.keys()


class IndexedDB:
    def __init__(self, manager: IndexedDBManager, db_name: str):
        self.__manager = manager
        self.__table_created = False
        self.__supports_jsonization: Optional[bool] = None
        self.__table = "`db_%s`" % db_name.replace('`', '``')

    def get(self, item: str, default: Optional[Any] = None) -> Any:
        try:
            return self[item]
        except KeyError:
            return default

    def key_exists(self, item: str) -> bool:
        return not not self.__manager.sqlitedb.execute(f"SELECT value FROM {self.__table} WHERE key=?", (item,))

    def keys(self) -> Set[str]:
        return set(map(lambda x: x[0], self.__manager.sqlitedb.execute(f"SELECT key FROM {self.__table}")))

    def values(self) -> Iterator[Any]:
        return map(lambda x: x[1], self.items())

    def items(self) -> Iterator[Tuple[str, Any]]:
        for k in self.keys():
            yield k, self[k]

    def drop_db(self) -> None:
        self.__manager.sqlitedb.execute(f"DROP TABLE {self.__table}")

    def __str__(self):
        return str({k: str(v) for k, v in self.items()})

    def __getitem__(self, item: str) -> Any:
        if self.__test_supports_jsonization():
            cmd = f"SELECT value, jsonized FROM {self.__table} WHERE key=?"
        else:
            cmd = f"SELECT value FROM {self.__table} WHERE key=?"

        r = self.__manager.sqlitedb.execute(cmd, (item,))
        if r:
            value = r[0][0]

            if self.__supports_jsonization and r[0][1]:
                value = json.loads(value)

            return value
        raise KeyError

    def __setitem__(self, key: str, value: Any) -> None:
        jsonize = type(value) is not str

        if jsonize:
            self.__support_jsonization()
            value = json.dumps(value)

        if self.key_exists(key):
            if self.__test_supports_jsonization():
                self.__manager.sqlitedb.execute(f"""
                UPDATE {self.__table} SET value=?, jsonized=? WHERE key=?
                """, (value, jsonize, key))
            else:
                self.__manager.sqlitedb.execute(f"UPDATE {self.__table} SET value=? WHERE key=?", (value, key))
        else:
            if self.__supports_jsonization:
                self.__manager.sqlitedb.execute(
                    f"INSERT INTO {self.__table} (`key`,`value`,`jsonized`) VALUES (?,?,?);", (key, value, jsonize)
                )
            else:
                self.__manager.sqlitedb.execute(
                    f"INSERT INTO {self.__table} (`key`,`value`) VALUES (?,?);", (key, value)
                )
        self.__manager.sqlitedb.commit()

    def __delitem__(self, key: str) -> None:
        self.__manager.sqlitedb.execute(
            f"DELETE FROM {self.__table} WHERE `key`=?;", (key,)
        )

    def __contains__(self, item: str):
        return item in self.keys()

    def __bool__(self) -> bool:
        return not not self.keys()

    def __create_table(self) -> None:
        if self.__table_created:
            return
        self.__manager.sqlitedb.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.__table} (
        `key`	TEXT NOT NULL UNIQUE,
        `value`	TEXT
        );
        """)
        self.__table_created = True

    def __test_supports_jsonization(self) -> bool:
        if self.__supports_jsonization is not None:
            return self.__supports_jsonization
        for _, column_name, _, _, _, _ in self.__manager.sqlitedb.execute(f"PRAGMA table_info({self.__table})"):
            if column_name == 'jsonized':
                self.__supports_jsonization = True
                return True
        self.__supports_jsonization = False
        return False

    def __support_jsonization(self) -> None:
        if self.__supports_jsonization or self.__test_supports_jsonization():
            return
        self.__manager.sqlitedb.execute(f"""
        ALTER TABLE {self.__table} ADD COLUMN jsonized INTEGER NOT NULL DEFAULT 0
        """)
        self.__supports_jsonization = True
