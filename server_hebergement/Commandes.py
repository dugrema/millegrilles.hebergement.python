import logging

from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages.MessagesModule import RessourcesConsommation
from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur, MessageWrapper

from millegrilles_web.Commandes import CommandHandler


class CommandHebergementHandler(CommandHandler):

    def __init__(self, web_app):
        super().__init__(web_app)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def configurer_consumers(self, messages_thread: MessagesThread):
        super().configurer_consumers(messages_thread)

        # Queue dynamique selon subscriptions (rooms) dans socketio
        res_subscriptions = RessourcesConsommation(
            self.socket_io_handler.subscription_handler.callback_reply_q,
            channel_separe=True, est_asyncio=True)
        self.socket_io_handler.subscription_handler.messages_thread = messages_thread
        self.socket_io_handler.subscription_handler.ressources_consommation = res_subscriptions

        messages_thread.ajouter_consumer(res_subscriptions)

    async def traiter_commande(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        routing_key = message.routing_key
        exchange = message.exchange
        action = routing_key.split('.').pop()
        type_message = routing_key.split('.')[0]
        enveloppe = message.certificat

        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        try:
            roles = enveloppe.get_roles
        except ExtensionNotFound:
            roles = list()

        try:
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            user_id = list()

        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None

        # if type_message == 'evenement':
        #     if exchange == Constantes.SECURITE_PRIVE:
        #         if action == 'activationFingerprintPk':
        #             return await self.socket_io_handler.traiter_message_userid(message)

        # Fallback sur comportement de la super classe
        return await super().traiter_commande(producer, message)

    async def traiter_cedule(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        await super().traiter_cedule(producer, message)

        # contenu = message.parsed
        # date_cedule = datetime.datetime.fromtimestamp(contenu['estampille'], tz=pytz.UTC)
        #
        # now = datetime.datetime.now(tz=pytz.UTC)
        # if now - datetime.timedelta(minutes=2) > date_cedule:
        #     return  # Vieux message de cedule
        #
        # weekday = date_cedule.weekday()
        # hour = date_cedule.hour
        # minute = date_cedule.minute
        #
        # if weekday == 0 and hour == 4:
        #     pass
        # elif minute % 20 == 0:
        #     pass

