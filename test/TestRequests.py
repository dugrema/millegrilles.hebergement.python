import requests
import json

from os import environ

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_reponse

PATH_CORE_CERT = '/var/opt/millegrilles/secrets/pki.core.cert'
PATH_CORE_CLE = '/var/opt/millegrilles/secrets/pki.core.cle'
PATH_CERT_CA = '/var/opt/millegrilles/configuration/pki.millegrille.cert'

HEBERGEMENT_HOST = environ.get('HEBERGEMENT_HOST')

if HEBERGEMENT_HOST is None:
    raise Exception('env param manquant : HEBERGEMENT_HOST')

clecert = CleCertificat.from_files(PATH_CORE_CLE, PATH_CORE_CERT)
enveloppe = clecert.enveloppe
idmg = enveloppe.idmg

ca = EnveloppeCertificat.from_file(PATH_CERT_CA)

signateur = SignateurTransactionSimple(clecert)
formatteur = FormatteurMessageMilleGrilles(idmg, signateur, ca)


def request_jwt():
    print('\nrequest_jwt\n***')
    message_1 = {'roles': ['fichiers']}
    message_signe, message_id = formatteur.signer_message(Constantes.KIND_REQUETE, message_1, 'Hebergement', action='getTokenJwt')
    message_signe['millegrille'] = ca.chaine_pem()[0]

    reponse = requests.get(f'https://{HEBERGEMENT_HOST}/hebergement/jwt', json=message_signe)
    print("Reponse : %s" % reponse.status_code)
    resultat = reponse.text
    print(resultat)
    resultat_json = json.loads(resultat)
    print(json.dumps(resultat_json, indent=2))

    # Dechiffrer la reponse
    reponse_dechiffree = dechiffrer_reponse(clecert, resultat_json)
    print("Reponse dechiffree: \n%s" % json.dumps(reponse_dechiffree, indent=2))

    return reponse_dechiffree


def request_fichier(jwt: str):
    print('\nrequest_consignation\n***')
    message_1 = {}

    fuuid = 'zSEfXUD5pMUsWysFfUTrVmjYkNX1LHfHUb62TYz7YxWd1yMVrbaMkSTA6YeGbcB7v5VTt58qkSfmXjKQ812oAjScZDfzFG'
    url_fichier = f'https://{HEBERGEMENT_HOST}/hebergement/fichiers/{fuuid}'

    headers = {'X-jwt': jwt}

    reponse = requests.get(url_fichier, json=message_1, headers=headers)
    print("Reponse : %s" % reponse.status_code)


def main():
    jwt = request_jwt()
    request_fichier(jwt['jwt_readonly'])
    # consignation = request_consignation(jwt['jwt_readonly'])


if __name__ == '__main__':
    main()
