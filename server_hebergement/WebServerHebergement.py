import logging

from millegrilles_web.WebServer import WebServer

from server_hebergement import Constantes as ConstantesHebergement
from server_hebergement.SocketIoHebergementHandler import SocketIoHebergementHandler


class WebServerHebergement(WebServer):

    def __init__(self, etat, commandes):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(ConstantesHebergement.WEBAPP_PATH, etat, commandes)

    def get_nom_app(self) -> str:
        return ConstantesHebergement.APP_NAME

    async def setup_socketio(self):
        """ Wiring socket.io """
        # Utiliser la bonne instance de SocketIoHandler dans une sous-classe
        self._socket_io_handler = SocketIoHebergementHandler(self, self._stop_event)
        await self._socket_io_handler.setup()

    async def _preparer_routes(self):
        self.__logger.info("Preparer routes %s sous /%s" % (self.__class__.__name__, self.get_nom_app()))
        await super()._preparer_routes()
