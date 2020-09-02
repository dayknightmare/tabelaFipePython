import mysql.connector
import urllib3
import json
import time

urlmodelo = "https://api.fipe.cenarioconsulta.com.br/modelos/"
urlanos = "https://api.fipe.cenarioconsulta.com.br/anos/"

db = mysql.connector.connect(
    
)

cursor = db.cursor()

http = urllib3.PoolManager()

s = time.time()
marcas = http.request("GET", 'https://api.fipe.cenarioconsulta.com.br/marcas/2')

for i in json.loads(marcas.data)['body']:
    cursor.execute("SELECT * FROM marcas_fipe WHERE id_marca = %s" % (i['IdMarca']))
    marcaX = cursor.fetchall()

    id = None

    if len(marcaX) > 0:
        cursor.execute("UPDATE marcas_fipe SET nome = '%s' WHERE id = %s" % (i['Marca'], marcaX[0][0]))
        db.commit()
        id = marcaX[0][0]

    else:
        cursor.execute("INSERT INTO marcas_fipe(id_marca, nome) VALUES(%s, '%s')" % (i['IdMarca'], i['Marca']))
        db.commit()
        id = cursor.lastrowid

    modelo = http.request("GET", urlmodelo + i['IdMarca'])

    for j in json.loads(modelo.data)['body']:
        cursor.execute("SELECT * FROM modelo_fipe WHERE id_modelo = %s" % (j['IdModelo']))
        modeloX = cursor.fetchall()

        IdModelo = None

        if len(modeloX) > 0:
            cursor.execute("UPDATE modelo_fipe SET nome = '%s', mes = '%s', nome_simples = '%s' WHERE id = %s" % (j['Modelo'], j['MesReferencia'], j['ModeloResumido'], modeloX[0][0]))
            db.commit()
            IdModelo = modeloX[0][0]

        else:
            cursor.execute("INSERT INTO modelo_fipe(nome, id_marca, mes, nome_simples, id_modelo) VALUES('%s', %s, '%s', '%s', %s)" % (j['Modelo'], id, j['MesReferencia'], j['ModeloResumido'], j['IdModelo']))
            db.commit()
            IdModelo = cursor.lastrowid

        anos = http.request('GET', urlanos + j['IdModelo'])

        for k in json.loads(anos.data)['body']:
            if k['Ano'] == 32000 or k['Ano'] == '32000':
                continue

            cursor.execute("SELECT * FROM anos_fipe WHERE id_ano = %s" % (k['IdAno']))
            anoX = cursor.fetchall()

            if len(anoX) > 0:
                cursor.execute('UPDATE anos_fipe SET ano = %s, mes_referencia = "%s", valor = %s, ordem = %s, combustivel = "%s" WHERE id = %s' % (k['Ano'], k['MesReferencia'], k['Valor'], k['Ordenacao'], k['combustivel'], anoX[0][0]))
                db.commit()

            else:
                cursor.execute('INSERT INTO anos_fipe(ano, id_modelo, mes_referencia, valor, ordem, combustivel, id_ano) VALUES(%s, %s, "%s", %s, %s, "%s", %s)' % (k['Ano'], IdModelo, k['MesReferencia'], k['Valor'], k['Ordenacao'], k['combustivel'], k['IdAno']))
                db.commit()
        else:
            print("\tFIM ANOS DE ", j['Modelo'])

    else:
        print("FIM MODELOS ", i['Marca'])

else:
    print("FIM MARCAS")

db.close()

print("Tempo gasto: ", time.time() - s)