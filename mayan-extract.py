import os
import sys
import shutil
import psycopg2

mayan_document_path = "/home/robert/Data/containers/mayan-edms/media/document_storage"
output_path = "/home/robert/Data/dms-extract"

copied = 0
errors = 0


def copyFiles(cabinetId, path):
    errors = 0
    copied = 0

    sql = "select file, label from documents_document d join documents_documentversion dv on d.id = dv.document_id join cabinets_cabinet_documents c on d.id = c.document_id where cabinet_id = " + str(cabinetId)
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        os.makedirs(output_path + "/" + path, exist_ok=True)
        source = mayan_document_path + "/" + row[0]
        dest = output_path  + path +"/" + row[1]
        try:
            shutil.copy(source, dest)
            copied += 1
        except Exception as e:
            print("Copy failed for " + source + " -> " + dest)
            print(str(e))
            errors += 1
    return (copied, errors)

def extractFiles(parent, path):
    global copied
    global errors
    cur = conn.cursor()
    query = "select id, label from cabinets_cabinet where parent_id"
    if parent == 0:
        query += " is null"
    else:
        query += " = " + str(parent)
    cur.execute(query)
    rows = cur.fetchall()
    print(".", end="")
    for row in rows:
        cabinetId = row[0]
        cabinetName = row[1]
        result = copyFiles(cabinetId, path + "/" + cabinetName )
        copied += result[0]
        errors += result[1]
        extractFiles(cabinetId, path + "/" + cabinetName)

try:
    conn = psycopg2.connect("host='localhost' dbname='mayan' user='mayan' password='mayanuserpass'")
except:
    print("Connection error")

cur = conn.cursor()
print("Extracting.", end="")
extractFiles(0, "")
print("done")
print(str(copied) +  " files extracted")
print(str(errors) + " errors")

conn.close()



