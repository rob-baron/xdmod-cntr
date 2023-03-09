#!/usr/local/bin/python3
""" Module to gzip and store a directory in the database """
import os
import datetime
import time
import json
import re
import argparse
import tempfile
import mysql.connector


def exec_fetchone(cursor, sql_stmt, params=None):
    """executes the sql stmt and fetches the first one in the result list"""
    cursor.execute(sql_stmt, params)
    result = cursor.fetchone()
    return result[0]


def connect_to_db(database):
    """This just connects to the database"""
    host = database["host"]
    admin_acct = "root"
    admin_pass = database["admin_password"]
    cnx = mysql.connector.connect(host=host, user=admin_acct, password=admin_pass)
    return cnx


def write_file_to_db(cursor, filename, script):
    """As a work-a-round for RWM, share config files though the database"""
    # it is ok if the file doesn't as the clouds.yaml is possibly empty or manually updated
    if os.path.isfile(filename):
        print(f" Writing {filename} to db")
        with open(filename, "rb") as file:
            file_contents = file.read()
            count = exec_fetchone(
                cursor,
                "select count(*) from file_share_db.file where script=%s",
                (script,),
            )
            if count == 0:
                cursor.execute(
                    "insert into file_share_db.file (script, file_name, file_data) values (%s,%s,%s)",
                    (script, filename, file_contents),
                )
            else:
                cursor.execute(
                    "update file_share_db.file set file_name=%s, file_data=%s where script=%s",
                    (filename, file_contents, script),
                )


def write_directory_to_db(dir_name, key_name):
    """Creates a gzip tarball to store in the database"""
    with open("/etc/xdmod/xdmod_init.json", encoding="utf-8") as json_file:
        xdmod_init_json = json.load(json_file)
        cnx = connect_to_db(xdmod_init_json["database"])
        tmp_dir = "/tmp/"
        tgz_file = f"{tmp_dir}tgz_file.tgz"
        b64_file = f"{tmp_dir}b65_file.b64"
        os.system(f"tar -czf {tgz_file} {dir_name}/*")
        os.system(f"base64 {tgz_file} > {b64_file}")
        write_file_to_db(cnx.cursor(), b64_file, key_name)
        os.system(f"rm {tgz_file}")
        os.system(f"rm {b64_file}")

        cnx.commit()
        cnx.close()


def sleep_till_midnight():
    """do an initial run, then run after midnight (after a good sleep)"""
    current_time = datetime.datetime.now()
    midnight = (current_time + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    # Indicate what time it is and how long we are sleeping for
    print(f"{current_time} sleeping for {(midnight - current_time).seconds}")
    time.sleep((midnight - current_time).seconds)


def key_from_dir(dir_name):
    """generates a key from the directory name"""
    ret_str = re.sub("^/|/$", "", dir_name)
    return re.sub("/", "-", ret_str).lower()


def process_directories(dir_list):
    """stores each directory in the list to the database"""
    for dir_name in dir_list:
        key_val = key_from_dir(dir_name)
        print("Processing {dir_name},{key_val}")
        write_directory_to_db(dir_name, key_val)


def process_continueously(dir_list):
    """this is just an infinte loop, to write the tarball to the db and wait"""
    while True:
        process_directories(dir_list)
        sleep_till_midnight()


def main():
    """sstore-to-db [--single_run] --directories <dir 1> [dir 2] ... [dir N]"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--single_run", action="store_true", required=False)
    parser.add_argument("--directories", type=str, nargs="+", required=True)
    args = parser.parse_args()


    if args.single_run:
        process_directories(args.directories)
    else:
        process_continueously(args.directories)


main()
