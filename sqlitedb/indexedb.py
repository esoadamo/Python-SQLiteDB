import json
import pickle
from base64 import b64encode, b64decode
from pathlib import Path
from random import choice
from typing import Union, Optional, Dict, Any, Set, Iterator, Tuple

from .sqlitedb import SQLiteDB


class IndexedDBManager:
    def __init__(self, file: Union[Path, str]) -> None:
        self.__db = SQLiteDB(file)
        self.__db_cache: Dict[str, "IndexedDB"] = {}

    def keys(self) -> Set[str]:
        tables = self.__db.execute("SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name LIKE 'db_%'")
        return {table[0][3:] for table in tables}

    def values(self) -> Iterator["IndexedDB"]:
        return map(lambda x: x[1], self.items())

    def items(self) -> Iterator[Tuple[str, "IndexedDB"]]:
        for k in self.keys():
            yield k, self[k]

    def to_dict(self) -> Dict[str, Any]:
        return {k: v.to_dict() for k, v in self.items()}

    def __str__(self):
        return str(self.to_dict())

    def __getitem__(self, item: str) -> "IndexedDB":
        if len(self.__db_cache) > 128:
            del self.__db_cache[choice(list(self.__db_cache.keys()))]
        if item not in self.__db_cache:
            self.__db_cache[item] = IndexedDB(self.__db, item)
        return self.__db_cache[item]

    def __delitem__(self, key: str) -> None:
        self[key].drop_db()

    def __contains__(self, item: str) -> bool:
        return item in self.keys()

    def __del__(self) -> None:
        self.__db.quit()

    def __bool__(self) -> bool:
        return not not self.keys()


class IndexedDB:
    def __init__(self, db: SQLiteDB, db_name: str):
        self.__db = db
        self.__table_created = False
        self.__supports_jsonization: Optional[bool] = None
        self.__table = "`db_%s`" % db_name.replace('`', '``')

    def get(self, item: str, default: Optional[Any] = None) -> Any:
        try:
            return self[item]
        except KeyError:
            return default

    def key_exists(self, item: str) -> bool:
        self.__create_table()
        return not not self.__db.execute(f"SELECT value FROM {self.__table} WHERE key=?", (item,))

    def keys(self) -> Set[str]:
        self.__create_table()
        return set(map(lambda x: x[0], self.__db.execute(f"SELECT key FROM {self.__table}")))

    def values(self) -> Iterator[Any]:
        return map(lambda x: x[1], self.items())

    def items(self) -> Iterator[Tuple[str, Any]]:
        for k in self.keys():
            yield k, self[k]

    def drop_db(self) -> None:
        self.__create_table()
        self.__db.execute(f"DROP TABLE {self.__table}")

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.items()}

    def __str__(self):
        return str(self.to_dict())

    def __getitem__(self, item: str) -> Any:
        self.__create_table()
        if self.__test_supports_jsonization():
            cmd = f"SELECT value, jsonized FROM {self.__table} WHERE key=?"
        else:
            cmd = f"SELECT value FROM {self.__table} WHERE key=?"

        r = self.__db.execute(cmd, (item,))
        if r:
            value: str = r[0][0]

            if self.__supports_jsonization:
                jsonized_state: int = r[0][1]
                if jsonized_state == 1:
                    value = json.loads(value)
                elif jsonized_state == 2:
                    value = pickle.loads(b64decode(value))

            return value
        raise KeyError

    def __setitem__(self, key: str, value: Any) -> None:
        self.__create_table()
        jsonized_status = 0

        if type(value) is not str:
            self.__support_jsonization()
            try:
                value = json.dumps(value)
                jsonized_status = 1
            except TypeError:
                value = b64encode(pickle.dumps(value)).decode('ascii')
                jsonized_status = 2

        if self.key_exists(key):
            if self.__test_supports_jsonization():
                self.__db.execute(f"""
                UPDATE {self.__table} SET value=?, jsonized=? WHERE key=?
                """, (value, jsonized_status, key))
            else:
                self.__db.execute(f"UPDATE {self.__table} SET value=? WHERE key=?", (value, key))
        else:
            if self.__supports_jsonization:
                self.__db.execute(
                    f"INSERT INTO {self.__table} (`key`,`value`,`jsonized`) VALUES (?,?,?);", (key, value, jsonized_status)
                )
            else:
                self.__db.execute(
                    f"INSERT INTO {self.__table} (`key`,`value`) VALUES (?,?);", (key, value)
                )
        self.__db.commit()

    def __delitem__(self, key: str) -> None:
        self.__create_table()
        self.__db.execute(
            f"DELETE FROM {self.__table} WHERE `key`=?;", (key,)
        )

    def __contains__(self, item: str):
        return item in self.keys()

    def __bool__(self) -> bool:
        return not not self.keys()

    def __create_table(self) -> None:
        if self.__table_created:
            return
        self.__db.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.__table} (
        `key`	TEXT NOT NULL UNIQUE,
        `value`	TEXT
        );
        """)
        self.__table_created = True

    def __test_supports_jsonization(self) -> bool:
        if self.__supports_jsonization is not None:
            return self.__supports_jsonization
        self.__create_table()
        for _, column_name, _, _, _, _ in self.__db.execute(f"PRAGMA table_info({self.__table})"):
            if column_name == 'jsonized':
                self.__supports_jsonization = True
                return True
        self.__supports_jsonization = False
        return False

    def __support_jsonization(self) -> None:
        if self.__supports_jsonization or self.__test_supports_jsonization():
            return
        self.__db.execute(f"""
        ALTER TABLE {self.__table} ADD COLUMN jsonized INTEGER NOT NULL DEFAULT 0
        """)
        self.__supports_jsonization = True
