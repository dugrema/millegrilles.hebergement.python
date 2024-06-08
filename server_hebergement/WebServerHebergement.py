import asyncio
import logging

from aiohttp import web
from typing import Optional

import redis

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_web.WebServer import WebServer
from millegrilles_web import Constantes as ConstantesWeb

from server_hebergement import Constantes as ConstantesHebergement
from server_hebergement.SocketIoHebergementHandler import SocketIoHebergementHandler
from server_hebergement.WebJwt import JwtHandler
from server_hebergement.WebConsignation import ConsignationHandler


class WebServerHebergement(WebServer):

    def __init__(self, etat, commandes):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(ConstantesHebergement.WEBAPP_PATH, etat, commandes)
        self.__jwt_handler: Optional[JwtHandler] = None
        self.__redis_session: Optional[redis.Redis] = None
        self.__consignation: Optional[ConsignationHandler] = None

    def get_nom_app(self) -> str:
        return ConstantesHebergement.APP_NAME

    async def setup(self, configuration: Optional[dict] = None, stop_event: Optional[asyncio.Event] = None):
        self.__redis_session = await self._connect_redis(ConstantesWeb.REDIS_DB_TOKENS)
        self.__jwt_handler = JwtHandler(self.etat, self.__redis_session)
        self.__consignation = ConsignationHandler(stop_event, self.etat)
        await self.__consignation.setup()

        await super().setup(configuration, stop_event)

    async def setup_socketio(self):
        """ Wiring socket.io """
        # Utiliser la bonne instance de SocketIoHandler dans une sous-classe
        self._socket_io_handler = SocketIoHebergementHandler(self, self._stop_event)
        await self._socket_io_handler.setup()

    async def _preparer_routes(self):
        self.__logger.info("Preparer routes %s sous /%s" % (self.__class__.__name__, self.get_nom_app()))
        await super()._preparer_routes()
        self._app.add_routes([
            web.get(f'{self.app_path}/auth', self.__jwt_handler.handle_auth),
            web.get(f'{self.app_path}/jwt', self.__jwt_handler.handle_get_jwt),
        ])
        self._app.add_routes(self.__consignation.get_routes(self.app_path))

    async def run(self):
        self.__logger.info("WebServeurHebergement.run Debut")
        tasks = [
            super().run(),
            self.__consignation.run()
        ]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        self.__logger.info("WebServeurHebergement.run Fin")
