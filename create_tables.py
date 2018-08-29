#!/usr/bin/env python3

import database

if __name__ == '__main__':
    db = database.Database()
    db.create_table()
    db.close()



