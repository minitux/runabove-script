#!/usr/bin/python 
from datetime import datetime,timedelta
import os 
import tarfile
from runabove import Runabove
import MySQLdb as _mysql
import hashlib

application_key = ""
application_secret = ""
consumer_key = ""
runabove_region = ""
runabove_storage_name = 'backup_sql'

output_dir = "/tmp"
sql_server = "localhost"
sql_username = "root"
sql_password = ""

now = datetime.today()
day = '%02d' % now.day 
date_day = now.strftime('%Y%m%d')

list_db = ['mysql',
           'information_schema',
          ]

def md5Checksum(filePath):
    with open(filePath, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            m.update(data)
        return m.hexdigest()

for db in list_db:
    con = _mysql.connect(sql_server, sql_username,sql_password, db)
    run = Runabove(application_key, application_secret, consumer_key=consumer_key)
    container = run.containers.create(runabove_region, runabove_storage_name)
    list_file = [] 
    with con:
        cur = con.cursor()
        cur.execute("SHOW TABLES;")
        rows = cur.fetchall() 
        for row in rows:
             table = row[0]
             filename = "%s/%s/%s_%s.sql.gz" % (output_dir,db,table,date_day)
             directory = os.path.dirname(filename)
             if os.path.isdir(directory) is False:
                 os.makedirs(directory)
             command = '/usr/bin/mysqldump --opt --hex-blob --force %s %s | gzip > %s' % (db, table, filename)
             os.system(command)

        for file in os.listdir(directory):
            if file.endswith(".sql.gz"):
                list_file.append(file)

        if list_file:
             tar_name = "%s/%s/%s.tar" % (output_dir,db,db)
             tar = tarfile.open(tar_name, "w")
             for name in list_file:
                 full_path = "%s/%s" % (directory,name)
                 tar.add(full_path, arcname=name)
             tar.close()
             md5_tar_name = md5Checksum(tar_name)
             object_name = "%s_%s/%s.tar" % (db,date_day,db)
             container.create_object(object_name, open(tar_name))
             object_info = container.get_object_by_name(object_name)
             remote_md5 = object_info.meta['etag'] 
             
	     if md5_tar_name == remote_md5:
                 os.remove(tar_name)

             for sqlgz in list_file:
                 os.remove("%s/%s" % (directory,sqlgz))
    if con:
        con.close()
