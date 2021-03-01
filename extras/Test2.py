import numpy as np
import random
import re
import datetime

if __name__ == '__main__':
    def validarCedula(id):
        id = '0011604650002C'
        LETRAS = "ABCDEFGHJKLMNPQRSTUVWXY"
        cedula = re.sub('-', '', id.strip())

        preFixValid = [
            '001',
            '002',
            '003',
            '004',
            '005',
            '006',
            '007',
            '008',
            '009',
            '041',
            '042',
            '043',
            '044',
            '045',
            '046',
            '047',
            '048',
            '081',
            '082',
            '083',
            '084',
            '085',
            '086',
            '087',
            '088',
            '089',
            '090',
            '091',
            '092',
            '093',
            '121',
            '122',
            '123',
            '124',
            '125',
            '126',
            '127',
            '128',
            '129',
            '130',
            '161',
            '162',
            '163',
            '164',
            '165',
            '166',
            '201',
            '202',
            '203',
            '204',
            '241',
            '242',
            '243',
            '244',
            '245',
            '246',
            '247',
            '281',
            '283',
            '284',
            '285',
            '286',
            '287',
            '288',
            '289',
            '290',
            '291',
            '321',
            '322',
            '323',
            '324',
            '325',
            '326',
            '327',
            '328',
            '329',
            '361',
            '362',
            '363',
            '364',
            '365',
            '366',
            '401',
            '401',
            '402',
            '402',
            '403',
            '403',
            '404',
            '404',
            '405',
            '405',
            '406',
            '406',
            '407',
            '407',
            '408',
            '408',
            '409',
            '409',
            '441',
            '442',
            '443',
            '444',
            '445',
            '446',
            '447',
            '448',
            '449',
            '450',
            '451',
            '452',
            '453',
            '454',
            '481',
            '482',
            '483',
            '484',
            '485',
            '486',
            '487',
            '488',
            '489',
            '490',
            '491',
            '492',
            '493',
            '521',
            '522',
            '523',
            '524',
            '525',
            '526',
            '561',
            '562',
            '563',
            '564',
            '565',
            '566',
            '567',
            '568',
            '569',
            '570',
            '601',
            '602',
            '603',
            '604',
            '605',
            '606',
            '607',
            '608',
            '610',
            '611',
            '612',
            '615',
            '616',
            '619',
            '624',
            '626',
            '627',
            '628',
            '888',
            '001',
            '002',
            '003',
            '004',
            '005',
            '006',
            '007',
            '008',
            '009',
        ]

        if (cedula == None or len(cedula) != 14):
            cedula = None
        else:
            cedula = cedula.upper()

        def getPrefijoCedula():
            if (cedula != None):
                return cedula[0:3]
            else:
                return None

        def searchPrf(prefijo):
            for pre in preFixValid:
                if pre == prefijo:
                    return True
            return False

        def isPrefijoValido():
            prefijo = getPrefijoCedula()
            if (not prefijo):
                return False
            if (not searchPrf(prefijo)):
                return False
            reg = '^[0-9]{3}$'

            if re.search(reg, prefijo) == None:
                return False

            return True

        def getFechaCedula():
            if (cedula != None):
                return cedula[3:9]
            else:
                return None

        def isFechaValida():
            fecha = getFechaCedula()

            if (fecha == None):
                return False

            reg = '^(0[1-9]|[12][0-9]|3[01])(0[1-9]|1[012])([0-9]{2})$'
            if re.search(reg, fecha) == None:
                return False

            return True

        def getSufijoCedula():
            if (cedula != None):
                return cedula[9:14]
            else:
                return None

        def isSufijoValido():
            sufijo = getSufijoCedula()

            if (sufijo == None):
                return False

            reg = '^[0-9]{4}[A-Y]$'
            if re.search(reg, sufijo) == None:
                return False

            return True

        def getCedulaSinLetra():
            if (cedula != None):
                return cedula[0:13]
            else:
                return None

        def getPosicionLetra():
            posicionLetra = 0
            cedulaSinLetra = getCedulaSinLetra()
            numeroSinLetra = 0

            if (cedulaSinLetra == None):
                return -1

            numeroSinLetra = cedulaSinLetra
            posicionLetra = float(numeroSinLetra) - math.floor(float(numeroSinLetra) / 23.0) * 23

            return posicionLetra

        def calcularLetra():
            posicionLetra = getPosicionLetra()

            if (posicionLetra < 0 or posicionLetra >= len(LETRAS)):
                return '?'

            return LETRAS[int(posicionLetra)]

        def getLetraCedula():
            if (cedula):
                return cedula[13:14]
            else:
                return None

        def isLetraValida():
            letraValida = None
            letra = getLetraCedula()
            if (letra == None):
                return None

            letraValida = calcularLetra()
            return letraValida == letra

        def isCedulaValida():
            if (not isPrefijoValido()):
                return False
            if (not isFechaValida()):
                return False
            if (not isSufijoValido()):
                return False
            if (not isLetraValida()):
                return False
            return True
            res = isCedulaValida()
            return res

        """ La fecha debe venir dd/mm/yy ejemplo 25/05/93 """
        def validateAge(dni):
        dni_dd = dni[3:5]
        dni_mm = dni[5:7]
        dni_yy = dni[7:9]

        if (int(dni_yy) > 40 and int(dni_yy) < 100):
            dni_yy = '19{}'.format(dni_yy)
        else:
            dni_yy = '20{}'.format(dni_yy)

        dd = datetime.today().strftime('%d')
        mm = datetime.today().strftime('%m')
        yy = datetime.today().strftime('%Y')
        years = int(yy) - int(dni_yy)

        if (int(mm) < int(dni_mm) or (int(mm) == int(dni_mm) and int(dd) < int(dni_dd))):
            years = years - 1
        
        if years > 17:
            return True
        else:
            return False

 
