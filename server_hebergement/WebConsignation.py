import asyncio
import aioredis
import logging

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event, BoundedSemaphore

from millegrilles_messages.messages import Constantes
from server_hebergement import Constantes as ConstantesHebergement


class ConsignationHandler:

    def __init__(self, etat):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._semaphore_threads = BoundedSemaphore(value=5)
        self.__etat = etat

    async def handle_get_consignation(self, request: Request):
        async with self._semaphore_threads:
            # Verifier instance-id du token JWT
            try:
                jwt = request.headers['X-jwt']
            except KeyError:
                return web.HTTPUnauthorized()

            # Transmettre requete au domaine hebergement
            try:
                producer = await asyncio.wait_for(self.__etat.producer_wait(), 3)
            except asyncio.TimeoutError:
                self.__logger.error("handle_get_consignation Timeout producer_wait")
                return web.HTTPServerError()

            requete_hebergement = {
            }
            try:
                reponse = await producer.executer_requete(
                    requete_hebergement, 'CoreTopologie', 'getConsignationFichiers', exchange=Constantes.SECURITE_PUBLIC)
                reponse = reponse.parsed
            except asyncio.TimeoutError:
                self.__logger.error("handle_get_jwt Timeout executer_requete")
                return web.HTTPServerError()

            data = {
                'consignation_url': reponse['consignation_url'] + '/hebergement',
            }

            champs_process = ['url_download', 'sync_intervalle']

            for champ in champs_process:
                try:
                    url_download = reponse[champ]
                    if url_download is not None and url_download != '':
                        data[champ] = url_download
                except KeyError:
                    pass

            return web.json_response(data)
