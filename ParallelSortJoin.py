#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import itertools
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'movieid1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'movieid1'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
pos_col = 0
int_table = []
def SortList(List):
    global pos_col
    global int_table
    List.sort(key=lambda tup: tup[pos_col])
    int_table.append(List)
    
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    global pos_col
    iptable = []
    cur = openconnection.cursor()
    cur.execute("SELECT MIN("+SortingColumnName+") FROM "+InputTable)
    cur.execute("SELECT * FROM "+InputTable)
    iptable = cur.fetchall()
    cur.execute("SELECT column_name,data_type FROM ddsassignment3.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+InputTable+"'")
    col_name = cur.fetchall()
    SortingColumnName = SortingColumnName.lower()
    pos_col = 0
    for i in range(len(col_name)):
        if col_name[i][0] == SortingColumnName:
            pos_col = i

    cur.execute("DROP TABLE IF EXISTS "+OutputTable)
    q = "CREATE TABLE "+OutputTable+" ("
    for i in col_name:
        q+=i[0]+" "+i[1]+","
    q=q[:-1]
    q+=")"
    cur.execute(q)

    #iptable - Input Table in List
    #pos_col - Position of column to be sorted

    optable = []
    jobs = []
    size_thread = len(iptable)/5

    for i in range(5):
        List = []
        if i != 4:
            List = iptable[size_thread*i:size_thread*(i+1)]
        else:
            List = iptable[size_thread*i:]
        thread = threading.Thread(target=SortList(List))
        jobs.append(thread)

    for j in jobs:
        j.start()
    for j in jobs:
        j.join()

    i0 = 0; i1 = 0; i2 = 0; i3 = 0; i4 = 0

    while(True):
        if i0<len(int_table[0]):
            v0 = int_table[0][i0][pos_col]
        else:
            v0 = 999999
        if i1<len(int_table[1]):
            v1 = int_table[1][i1][pos_col]
        else:
            v1 = 999999
        if i2<len(int_table[2]):
            v2 = int_table[2][i2][pos_col]
        else:
            v2 = 999999
        if i3<len(int_table[3]):
            v3 = int_table[3][i3][pos_col]
        else:
            v3 = 999999
        if i4<len(int_table[4]):
            v4 = int_table[4][i4][pos_col]
        else:
            v4 = 999999

        if v0 == -1 and v1 == -1 and v2 == -1 and v3 == -1 and v4 == -1:
            break
        
        m = min(v0,v1,v2,v3,v4)
        if m == 999999:
            break
        if v0 == m:
            optable.append(int_table[0][i0])
            i0+=1
        elif v1 == m:
            optable.append(int_table[1][i1])
            i1+=1
        elif v2 == m:
            optable.append(int_table[2][i2])
            i2+=1
        elif v3 == m:
            optable.append(int_table[3][i3])
            i3+=1
        elif v4 == m:
            optable.append(int_table[4][i4])
            i4+=1

    for i in optable:
        q = "INSERT INTO "+OutputTable+" VALUES("
        for j in range(len(col_name)):
            if col_name[j][1] != "character varying":
                q+=str(i[j])+","
            else:
                q+="'"+str(i[j])+"' "+","
        q = q[:-1]
        q+=")"
        cur.execute(q)
    openconnection.commit()


j_list = []
def JoinList(Table1, Table2, pos1, pos2, flag):
    global j_list

    if flag == 1:
        for i in range(len(Table1)):
            for j in range(len(Table2)):
                if Table1[i][pos1] == Table2[j][pos2]: 
                    newTup2 = ()
                    newTup1 = (Table1[i][pos1],)
                    for z in range(len(Table1[i])):
                        if z!= pos1:
                            newTup1 += (Table1[i][z],)
                    for z in range(len(Table2[j])):
                        if z!= pos2:
                            newTup2 += (Table2[j][z],)
                    newRow = newTup1 + newTup2
                    j_list.append(newRow)
    else:
        for i in range(len(Table1)):
            for j in range(len(Table2)):
                if Table1[i][pos1] == Table2[j][pos2]:
                    newTup1 = ()
                    newTup2 = ()
                    for z in Table1[i]:
                        newTup1 += (z,)
                    for z in Table2[j]:
                        newTup2 += (z,)
                    newRow = newTup1 + newTup2
                    j_list.append(newRow)
    
            
    
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    global int_table
    int_table = []
    ParallelSort(InputTable1, Table1JoinColumn, InputTable1, openconnection)
    int_table = []
    ParallelSort(InputTable2, Table2JoinColumn, InputTable2, openconnection)
    
    cur = openconnection.cursor()

    cur.execute("select m.column_name from (SELECT column_name FROM information_schema.columns WHERE table_name = '"+InputTable1+"' ) m , (SELECT column_name FROM information_schema.columns WHERE table_name = '"+InputTable2+"' ) n where m=n and m.column_name<>'"+Table1JoinColumn+"' and n.column_name<>'"+Table2JoinColumn+"';")
    rows = cur.fetchall()

    for row in rows:
        cur.execute("ALTER TABLE "+InputTable1+" RENAME COLUMN "+row[0]+" TO "+"one"+"_"+row[0]+"");
        cur.execute("ALTER TABLE "+InputTable2+" RENAME COLUMN "+row[0]+" TO "+"two"+"_"+row[0]+"");

    cur.execute("SELECT column_name,data_type FROM ddsassignment3.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+InputTable1+"'")
    col_name1 = cur.fetchall()
    cur.execute("SELECT column_name,data_type FROM ddsassignment3.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+InputTable2+"'")
    col_name2 = cur.fetchall()

    pos1 = 0
    pos2 = 0

    '''
    f = 0
    col_name1 = []
    for i in range(len(col_name1t)):
        for j in range(len(col_name2t)):
            if col_name1t[i][0] == col_name2t[j][0]:
                f = 1

        if f == 1:
            tup = (col_name1t[i][0]+"1",col_name1t[i][1])
        else:
            tup = (col_name1t[i][0],col_name1t[i][1])
        col_name1.append(tup)

    f = 0
    col_name2 = []
    for i in range(len(col_name2t)):
        for j in range(len(col_name1t)):
            if col_name2t[i][0] == col_name1t[j][0]:
                f = 1

        if f == 1:
            tup = (col_name2t[i][0]+"2",col_name2t[i][1])
        else:
            tup = (col_name2t[i][0],col_name2t[i][1])
        col_name2.append(tup)
    '''
                
                
    for i in range(len(col_name1)):
        if col_name1[i][0] == Table1JoinColumn.lower():
            pos1 = i
    for i in range(len(col_name2)):
        if col_name2[i][0] == Table2JoinColumn.lower():
            pos2 = i

    tab_lst = []
    cur.execute("DROP TABLE IF EXISTS "+OutputTable)
    q = "CREATE TABLE "+OutputTable+" ("
    if Table1JoinColumn.lower() == Table2JoinColumn.lower():
        for i in col_name1:
            if i[0] == Table1JoinColumn.lower():
                q+=i[0]+" "+i[1]+","
                tab_lst.append(i[1])
                
        for i in col_name1:
            if i[0] != Table1JoinColumn.lower():
                q+=i[0]+" "+i[1]+","
                tab_lst.append(i[1])

        for i in col_name2:
            if i[0] != Table2JoinColumn.lower():
                q+=i[0]+" "+i[1]+","
                tab_lst.append(i[1])
    else:
        for i in col_name1:
            q+=i[0]+" "+i[1]+","
            tab_lst.append(i[1])
        for i in col_name2:
            q+=i[0]+" "+i[1]+","
            tab_lst.append(i[1])
        
    q=q[:-1]
    q+=")"
    cur.execute(q)
    openconnection.commit()

    #Perform Join and insert into OutputTable
    cur.execute("SELECT max("+Table1JoinColumn+") - min("+Table1JoinColumn+") FROM "+InputTable1)
    range_col1 = cur.fetchall()[0][0]

    cur.execute("SELECT max("+Table2JoinColumn+") - min("+Table2JoinColumn+") FROM "+InputTable2)
    range_col2 = cur.fetchall()[0][0]
    
    range_col = max(range_col1,range_col2)
    bucket = range_col/5

    jobs = []
    start = 0
    end = bucket
    flag = 0
    if Table1JoinColumn == Table2JoinColumn:
        flag = 1
    for i in range(5):
        if i != 4:
            cur.execute("SELECT * FROM "+InputTable1+" WHERE "+Table1JoinColumn+">"+str(start)+" AND "+Table1JoinColumn+"<="+str(end))
        else:
            cur.execute("SELECT * FROM "+InputTable1+" WHERE "+Table1JoinColumn+">"+str(start))
        Table1 = cur.fetchall()
        if i != 4:
            cur.execute("SELECT * FROM "+InputTable2+" WHERE "+Table2JoinColumn+">"+str(start)+" AND "+Table2JoinColumn+"<="+str(end))
        else:
            cur.execute("SELECT * FROM "+InputTable2+" WHERE "+Table2JoinColumn+">"+str(start))
        Table2 = cur.fetchall()
        start += bucket
        end += bucket
        
        thread = threading.Thread(target=JoinList(Table1,Table2,pos1,pos2,flag))
        jobs.append(thread)

    for j in jobs:
	j.start()

    for j in jobs:
        j.join()

    for z in j_list:
        q = "INSERT INTO "+OutputTable+" VALUES("
        cnt = 0
        for val in z:
            if tab_lst[cnt] == "character varying":
                q += "'"+str(val)+"',"
            else:
                q += str(val)+","
            cnt += 1
        q = q[:-1]
        q+=")"
        cur.execute(q)
    openconnection.commit()
    
    

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
