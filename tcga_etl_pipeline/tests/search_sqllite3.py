'''
Created on Jun 9, 2016

@author: michael
'''
from datetime import datetime
import sys
import sqlite3

def main(dbname, sql):
    con = sqlite3.connect(dbname)
    try:
        cursor = con.cursor()
        cursor.execute(sql)
        print '%s' % ('\t'.join(x[0] for x in cursor.description))
        count = 20
        for row in cursor:
            print '%s' % ('\t'.join(str(value) for value in row))
            count -= 1
            if 0 == count:
                break
    finally:
        con.close()

if __name__ == '__main__':
    if 1 == len(sys.argv):
        print '%s: start sqlite3 select' % (datetime.now())
        con = sqlite3.connect('test.db')
        try:
            con.execute('drop table if exists test')
            con.execute("create table test (first varchar(10), second int, third float, fourth date);")
            inserts = [
                ('one', 1, 1.1, '2016-01-01 12:12:12'), \
                ('two', 2, 2.2, '2016-02-02 12:12:12'), \
                ('three', 3, 3.3, '2016-03-03 12:12:12'), \
                ('four', 4, 4.4, '2016-04-04 12:12:12') \
            ]
            con.executemany("insert into test (first, second, third, fourth) values (?,?,?,?)", inserts)
            con.commit()
        finally:
            con.close()
        main('test.db', 'test')
        print '%s: completed sqlite3 select' % (datetime.now())
    else:
        print '%s: start sqlite3 select for %s:%s' % (datetime.now(), sys.argv[1], sys.argv[2])
        main(sys.argv[1], sys.argv[2])
        print '%s: completed sqlite3 select' % (datetime.now())
