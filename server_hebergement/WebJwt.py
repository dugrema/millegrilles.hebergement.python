import aioredis
import asyncio
import logging

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event, BoundedSemaphore

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.ValidateurCertificats import valider_certificat_tiers
from millegrilles_messages.messages.ValidateurMessage import verifier_signature

from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web.JwtUtils import get_headers, verify
from server_hebergement import Constantes as ConstantesHebergement


class JwtHandler:

    def __init__(self, etat: EtatWeb, redis_session: aioredis.Redis):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat = etat
        self._redis_session = redis_session
        self._semaphore_threads = BoundedSemaphore(value=20)

    async def handle_auth(self, request: Request):
        async with self._semaphore_threads:
            path_request = request.headers['X-Original-URI']
            methode = request.headers['X-Original-Method']
            self.__logger.debug("headers : %s" % [h for h in request.headers.items()])

            try:
                jwt = request.headers[ConstantesHebergement.HEADER_JWT]
            except KeyError:
                self.__logger.info("handle_auth JWT manquant")
                return web.HTTPForbidden()

            jwt_headers = get_headers(jwt)
            kid = jwt_headers['kid']
            enveloppe = await self.__etat.charger_certificat(kid)
            jwt_contenu = verify(enveloppe, jwt)

            if jwt_contenu['iss'] != 'Hebergement':
                self.__logger.error("handle_auth Domaine (iss) doit etre hebergement")
                return web.HTTPForbidden()

            readwrite = jwt_contenu.get('readwrite') is True
            if methode in ['PUT']:
                if readwrite is False:
                    self.__logger.error("handle_auth Methode %s requiere readwrite = True dans JWT" % methode)
                    return web.HTTPForbidden()

            # Verifier autorisation par path
            if path_request.startswith('/hebergement/fichiers'):
                if methode in ['POST'] and readwrite is False:
                    self.__logger.error("handle_auth Methode %s sur fichiers requiere readwrite = True dans JWT" % methode)
                    return web.HTTPForbidden()

                if 'fichiers' in jwt_contenu['roles']:
                    return web.HTTPOk()
                else:
                    self.__logger.error("handle_auth Path /hebergement/fichiers requiert role fichiers dans JWT")

            return web.HTTPForbidden()

    async def handle_get_jwt(self, request: Request):
        async with self._semaphore_threads:

            # Verifier le certificat de la millegrille tierce. S'assurer que le message est bien forme et
            # que le certificat est valide.
            try:
                requete = await request.json()
                certificat_pem = requete['certificat']
                certificat_millegrille = requete['millegrille']

                # Valider le certificat
                enveloppe_millegrille = EnveloppeCertificat.from_pem(certificat_millegrille)
                enveloppe_certificat = valider_certificat_tiers(enveloppe_millegrille, certificat_pem)
                idmg = enveloppe_millegrille.idmg
                if Constantes.SECURITE_SECURE not in enveloppe_certificat.get_exchanges:
                    self.__logger.error("handle_get_jwt Acces refuse : certificat tiers pour JWT doit etre 4.secure")
                    return web.HTTPForbidden()
            except:
                self.__logger.exception("handle_get_jwt Erreur chargement request")
                return web.HTTPBadRequest()

            # Verifier le message. Le certificat a deja ete verifie.
            try:
                await self.__etat.validateur_message.verifier(requete, verifier_certificat=False)
            except:
                self.__logger.exception("handle_get_jwt Erreur verification signature")
                return web.HTTPBadRequest()

            # Transmettre requete au domaine hebergement
            try:
                producer = await asyncio.wait_for(self.__etat.producer_wait(), 3)
            except asyncio.TimeoutError:
                self.__logger.error("handle_get_jwt Timeout producer_wait")
                return web.HTTPServerError()

            requete_hebergement = {
                'requete': requete,
                'idmg': idmg,
            }
            try:
                reponse = await producer.executer_requete(
                    requete_hebergement, ConstantesHebergement.NOM_DOMAINE, 'getTokenJwt', exchange=Constantes.SECURITE_PUBLIC)
                reponse = reponse.original
                return web.json_response(reponse)
            except asyncio.TimeoutError:
                self.__logger.error("handle_get_jwt Timeout executer_requete")
                return web.HTTPServerError()
