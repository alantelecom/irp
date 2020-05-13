import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import json 
import re
from pymongo import MongoClient
import schedule
import time


def job():

    lista = []
    
    r = requests.get("https://www2.comprasnet.gov.br/siasgnet-irp/consultarIRPComprasNetSubmit.do?filtrosSelecionados=3%2C5%2C&servicoInteresse.itemCatalogo.nomeFormatado=-&nomeFuncaoRetorno=&uasgGerenciadora.numeroUasg=&servicoInteresse.itemCatalogo.categoria=&filtro=3&filtro=5&materialInteresse.itemCatalogo.tipo=M&servicoInteresse.itemCatalogo.nome=&municipioUasgGerenciadora.codigoMunicipio=&materialInteresse.itemCatalogo.nome=INFORM%C3%81TICA+-+EQUIPAMENTOS%2C++PE%C3%87AS%2C+ACESS%C3%93RIOS+E+SUPRIMENTOSDE+TIC&materialInteresse.itemCatalogo.nomeFormatado=70-INFORM%C3%81TICA+-+EQUIPAMENTOS%2C++PE%C3%87AS%2C+ACESS%C3%93RIOS+E+SUPRIMENTOSDE+TIC&servicoInteresse.itemCatalogo.codigo=&numeroIrp=&municipioUasgGerenciadora.municipioFormatado=&numeroPagina=1&materialInteresse.itemCatalogo.categoria=G&materialInteresse.itemCatalogo.codigo=70&situacoesIrp=2&situacoesIrp=3&situacoesIrp=4&servicoInteresse.itemCatalogo.tipo=&uasgGerenciadora.nome=&method=consultarIRPComprasNet", verify=False)
    page = bs(r.text, "html.parser")
    seletor = page.find_all("span", {"class":"pagebanner"})[0].text.split(" ")[0]
    
    if int(seletor)%20 == 0:
        quantIRP = int(int(seletor)/20)
    else:
        quantIRP = int(int(seletor)/20) + 1


    def detalhes(num):
        
        x = requests.get("https://www2.comprasnet.gov.br/siasgnet-irp/resumoIRP.do?numeroPagina=1&irp.codigoIrp={}&method=iniciar&acessoPublico=1".format(num), verify=False)
        pagin = bs(x.text, "html.parser")
        qPags = 1
        ultimaPage = "Alan"
        try: 
            ultimaPage = pagin.select_one(".pagelinks").select("a")[-1].get("href")
            qPags = re.findall("[0-9]+", ultimaPage)[-3]
        except:
            qPags = 1 
                        
        materiais = []                
        for i in range (1, int(qPags)+1):
            req = requests.get("https://www2.comprasnet.gov.br/siasgnet-irp/resumoIRP.do?numeroPagina={0}&irp.codigoIrp={1}&method=iniciar&acessoPublico=1".format(i, num), verify=False)
            pageMateriais = bs(req.text, "html.parser")
            tabela = pageMateriais.find(id='listaItensIRP')
            quantLinhasMat = tabela.find("tbody").find_all("tr")
            dfMat = pd.read_html(str(tabela))[0]
            mat = dfMat.to_dict()
            for j in range(0, len(quantLinhasMat),2):
                m = {
                    "Material": mat["Item"][j],
                    "Valor": str(mat["Valor Unitário Estimado (R$)"][j]) 
                }
                materiais.append(m)
        return materiais
        

    def pagina(pag):
    
        r = requests.get("https://www2.comprasnet.gov.br/siasgnet-irp/consultarIRPComprasNetSubmit.do?filtrosSelecionados=3%2C5%2C&servicoInteresse.itemCatalogo.nomeFormatado=-&nomeFuncaoRetorno=&uasgGerenciadora.numeroUasg=&servicoInteresse.itemCatalogo.categoria=&filtro=3&filtro=5&materialInteresse.itemCatalogo.tipo=M&servicoInteresse.itemCatalogo.nome=&municipioUasgGerenciadora.codigoMunicipio=&materialInteresse.itemCatalogo.nome=INFORM%C3%81TICA+-+EQUIPAMENTOS%2C++PE%C3%87AS%2C+ACESS%C3%93RIOS+E+SUPRIMENTOSDE+TIC&materialInteresse.itemCatalogo.nomeFormatado=70-INFORM%C3%81TICA+-+EQUIPAMENTOS%2C++PE%C3%87AS%2C+ACESS%C3%93RIOS+E+SUPRIMENTOSDE+TIC&servicoInteresse.itemCatalogo.codigo=&numeroIrp=&municipioUasgGerenciadora.municipioFormatado=&numeroPagina={}&materialInteresse.itemCatalogo.categoria=G&materialInteresse.itemCatalogo.codigo=70&situacoesIrp=2&situacoesIrp=3&situacoesIrp=4&servicoInteresse.itemCatalogo.tipo=&uasgGerenciadora.nome=&method=consultarIRPComprasNet".format(pag), verify=False)
        pagina = bs(r.text, "html.parser")
        nPrincipal = pagina.find(id='listaIrps')
        quantLinhas = nPrincipal.find("tbody").find_all("tr")
        df = pd.read_html(str(nPrincipal))[0]
        a = df.to_dict()
            
        for i in range(0, len(quantLinhas)):
            link = nPrincipal.find("tbody").find_all("tr")[i].find_all("td")[5].find("a")["onclick"]
            numLink = re.sub('[^0-9]', '', link)
            b = {
                "UASG": a["UASG Gerenciadora"][i],
                "Data": a["Data Provável da Licitação"][i],
                "Situação": a["Situação da IRP"][i],
                "Nº IRP": a["N° da IRP"][i],
                "Objeto": a["Objeto"][i],
                "Materiais": detalhes(numLink)
            }
            lista.append(b)    
        
    for i in range (1, quantIRP + 1):
        pagina(i)
    colecao.drop()
    time.sleep(2)
    colecao.insert_many(lista)

job()




