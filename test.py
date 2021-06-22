import requests 
from pathlib import Path
from xml.etree import ElementTree
if __name__ == '__main__':
    #URL for the Muhimbi Conversion Server
    address = 'http://172.16.188.169:3072/wsBancaMA.asmx?op=BancaMA'
     
    #We need to specify the SOAPAction and the content type
    #in the HTTP header. 
    headers = { 'Content-Type':'text/xml'}
     
    #We need to specify the
    body = """<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
    <BancaMA xmlns="http://www.e-Solutions.com.gt/WS/MW">
        <intCanalIn>1</intCanalIn>
        <intCanalOut>1</intCanalOut>
        <strReference>7595</strReference>
        <intTipo>193</intTipo>
        <strXML><![CDATA[ <Transaccion id="193"><Parametro nombre="LC_CODIGO_TRAMA">193</Parametro><Parametro nombre="LC_INGNIU">1774705260102</Parametro><Parametro nombre="LC_DATO1">1001</Parametro></Transaccion>]]></strXML>
        </BancaMA>
    </soap:Body>
    </soap:Envelope>"""
  
    try:
        # We use HTTP POST to call this function
        response = requests.post(address, headers=headers,data=body)
        dom = ElementTree.fromstring(response.content)
        print(dom)
        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'a': 'http://services.muhimbi.com/2009/10/06',
        }
        #Parsing the XML to get filecontent
        names = dom.findall(
        './soap:Body'
        '/a:ConvertResponse'
        '/a:ConvertResult',
        namespaces,
        )
        print(names)
    