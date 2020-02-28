import pymysql.cursors


# Функция возвращает connection.
def getConnection(globalC):
    # Вы можете изменить параметры соединения.

    connection = pymysql.connect(host=globalC[0],
                                 user=globalC[1],
                                 password=globalC[2],
                                 db=globalC[3],
                                 charset=globalC[4],
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def creatDB(globalC,arg):
    connection = getConnection(globalC)
    sql='CREATE TABLE `'+arg+'` ( `id` INT(11) NOT NULL AUTO_INCREMENT ,`idUser` INT(11) NOT NULL , `userName` VARCHAR(255) NOT NULL , `countWord` INT(11) NOT NULL , `colMate` INT(11) NOT NULL , `colGood` INT(11) NOT NULL , `stat` INT(11) NOT NULL , PRIMARY KEY (`id`) USING BTREE) ENGINE = InnoDB;'
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    finally:
        connection.close()

def selectDB(globalC,columnPrephix,column = 'idUser',table = 'users'):
    connection = getConnection(globalC)
    sql='SELECT * FROM `'+table+'` WHERE '+column+' =\''+columnPrephix+'\''

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return result[0]
    finally:
        connection.close()

def deleteDB(globalC, columnPrephix, column='idUser', table='users'):
    connection = getConnection(globalC)
    sql = 'DELETE FROM `'+table+'` WHERE '+column+' =\''+columnPrephix+'\''

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        return
    finally:
        connection.close()

def insertDB(globalC,arg, table='users'):
    connection = getConnection(globalC)
    sql = 'INSERT INTO `'+table+'`(`idUser`,`userName`, `countWord`, `colMate`, `colGood`, `stat`) VALUES (%s,%s,%s,%s,%s,%s)'

    try:
        cursor = connection.cursor()
        cursor.executemany(sql,arg)
        connection.commit()

    finally:
        connection.close()

def updateDB(globalC, arg,column = 'idUser',table='users'):
    connection = getConnection(globalC)
    sql = 'UPDATE `'+table+'` SET  idUser=%s, userName=%s, countWord = %s, colMate=%s,colGood=%s,stat=%s WHERE '+column+'='+arg[0][0]+''

    try:
        cursor = connection.cursor()
        cursor.executemany(sql,arg)
        connection.commit()


    finally:
        connection.close()

def selectColumnDB(globalC,researchColumn,columnPrephix=1,column = 'idUser',table = 'users'):
    connection = getConnection(globalC)
    if columnPrephix == 1:
        sql = 'SELECT ' + researchColumn + ' FROM `' + table + '` WHERE 1'
        print(sql)
    else:
        sql='SELECT ' +researchColumn+' FROM `'+table+'` WHERE '+column+ ' =\'' +columnPrephix+ '\''
        print(sql)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return result
    finally:
        connection.close()

def update (globalC,userId,username,countWord,colMate,colGood):
    select = selectDB(globalC,userId)
    arg=[userId]
    if select["userName"]!= username:
        arg.append(username)
    else:
        arg.append(select["userName"])

    if select["countWord"] != countWord:
        var = int(countWord) + select["countWord"]
        arg.append(str(var))
    else:
        arg.append(select["countWord"])

    if select["colMate"] != colMate:
        var = int(colMate) + select["colMate"]
        arg.append(str(var))
    else:
        arg.append(select["colMate"])

    if select["colGood"] != colGood:
        var = int(colGood)+select["colGood"]
        arg.append(str(var))
    else:
        arg.append(select["colGood"])
    valveWord = arg[2]
    badWord = arg[3]
    stat = str((int(badWord)/int(valveWord)) *100)
    if select["stat"] != stat:
        arg.append(stat)
    else:
        arg.append(select["stat"])
    args=[arg]
    print(args)
    updateDB(globalC,args)



