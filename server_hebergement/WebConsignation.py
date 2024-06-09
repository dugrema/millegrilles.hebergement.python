import asyncio
import aioredis
import datetime
import json
import logging
import pathlib
import shutil

from typing import Optional

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event, BoundedSemaphore

from millegrilles_messages.messages import Constantes
from millegrilles_web.TransfertFichiers import IntakeFichiers
from millegrilles_web.JwtUtils import get_headers, verify
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage

from server_hebergement import Constantes as ConstantesHebergement


class JobVerifierParts:

    def __init__(self, transaction, path_upload: pathlib.Path, hachage: str, cles: Optional[dict] = None):
        self.transaction = transaction
        self.path_upload = path_upload
        self.hachage = hachage
        self.cles = cles
        self.done = asyncio.Event()
        self.valide: Optional[bool] = None
        self.exception: Optional[Exception] = None


class ConsignationHandler:

    def __init__(self, stop_event: Optional[asyncio.Event], etat):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._semaphore_threads = BoundedSemaphore(value=5)
        self.__stop_event = stop_event
        self.__etat = etat
        self.__queue_verifier_parts: Optional[asyncio.Queue] = None
        self.__intake = IntakeFichiers(stop_event, etat)

    async def setup(self):
        await self.__intake.configurer()

    def get_routes(self, app_path):
        path_base_fichiers = f'{app_path}/fichiers'
        return [
            # /fichiers_transfert
            web.get('%s/job/{fuuid}' % path_base_fichiers, self.handle_get_job_fuuid),
            web.get('%s/{fuuid}' % path_base_fichiers, self.handle_get_fuuid),
            web.put('%s/{fuuid}/{position}' % path_base_fichiers, self.handle_put_fuuid),
            web.post('%s/{fuuid}' % path_base_fichiers, self.handle_post_fuuid),
            web.delete('%s/{fuuid}' % path_base_fichiers, self.handle_delete_fuuid),

            # /sync
            #web.get('/fichiers_transfert/sync/{fichier}', self.handle_get_fichier_sync),

            # /backup
            web.post('%s/backup/verifierFichiers' % path_base_fichiers, self.handle_post_backup_verifierfichiers),
            web.put('%s/backup/upload/{uuid_backup}/{domaine}/{nomfichier}' % path_base_fichiers, self.handle_put_backup),
            web.get('%s/backup/{uuid_backup}/{domaine}/{nomfichier}' % path_base_fichiers, self.handle_get_backup)
        ]

    def get_path_upload_fuuid(self, cn: str, fuuid: str):
        return pathlib.Path(self.__etat.configuration.dir_staging, ConstantesHebergement.DIR_STAGING_UPLOAD, cn, fuuid)

    async def handle_get_fuuid(self, request: Request):
        async with self._semaphore_threads:
            raise NotImplementedError("todo")

    async def handle_get_job_fuuid(self, request: Request):
        async with self._semaphore_threads:
            fuuid = request.match_info['fuuid']
            method = request.method
            self.__logger.debug("handle_get_fuuid method %s, fuuid %s" % (method, fuuid))

            # Lire JWT pour recuperer le idmg (sub). C'est aussi une revalidation.
            try:
                jwt_contenu = await parse_jwt(self.__etat, request.headers['X-jwt'])
                idmg = jwt_contenu['sub']
            except:
                self.__logger.exception("Erreur verification JWT")
                return web.HTTPForbidden()

            # TODO Verifier si le fichier existe dans la consignation (destination)
            # try:
            #     info = await self.__consignation.get_info_fichier(fuuid)
            #     if info.get('etat_fichier') != Constantes.DATABASE_ETAT_MANQUANT:
            #         # Le fichier existe et le traitement est complete
            #         return web.json_response({'complet': True})
            # except (TypeError, AttributeError, KeyError):
            #     pass  # OK, le fichier n'existe pas

            # Verifier si le fichier est dans l'intake
            path_intake = self.__intake.get_path_intake_fuuid(fuuid)
            if path_intake.exists():
                return web.json_response({'complet': False, 'en_traitement': True}, status=201)

            # Verifier si la job existe
            path_upload = self.get_path_upload_fuuid(idmg, fuuid)
            if path_upload.exists():
                # La job existe, trouver a quelle position du fichier on est rendu.
                part_max = 0
                position_courante = 0
                for fichier in path_upload.iterdir():
                    if fichier.name.endswith('.part'):
                        part_position = int(fichier.name.split('.')[0])
                        if part_position >= part_max:
                            part_max = part_position
                            stat_fichier = fichier.stat()
                            position_courante = part_position + stat_fichier.st_size

                return web.json_response({'complet': False, 'position': position_courante})

            # Ok, le fichier et la job n'existent pas
            return web.HTTPNotFound()

    async def handle_put_fuuid(self, request: Request):
        async with self._semaphore_threads:
            fuuid = request.match_info['fuuid']
            position = request.match_info['position']
            headers = request.headers

            # Afficher info (debug)
            self.__logger.debug("handle_put_fuuid fuuid: %s position: %s" % (fuuid, position))
            for key, value in headers.items():
                self.__logger.debug('handle_put_fuuid key: %s, value: %s' % (key, value))

            # Lire JWT pour recuperer le idmg (sub). C'est aussi une revalidation.
            try:
                jwt_contenu = await parse_jwt(self.__etat, request.headers['X-jwt'])
                idmg = jwt_contenu['sub']
            except:
                self.__logger.exception("Erreur verification JWT")
                return web.HTTPForbidden()

            content_hash = headers.get('x-content-hash')
            try:
                content_length = int(headers['Content-Length'])
            except KeyError:
                content_length = None

            # Creer repertoire pour sauvegader la partie de fichier
            path_upload = self.get_path_upload_fuuid(idmg, fuuid)
            path_upload.mkdir(parents=True, exist_ok=True)

            path_fichier = pathlib.Path(path_upload, '%s.part' % position)
            # S'assurer que le fichier .part n'existe pas deja (on serait en mode resume)
            try:
                stat_fichier = path_fichier.stat()
                if content_length == stat_fichier.st_size:
                    return web.HTTPOk()  # On a deja ce .part de fichier, il a la meme longueur
            except FileNotFoundError:
                pass  # Fichier absent ou taille differente

            path_fichier_work = pathlib.Path(path_upload, '%s.part.work' % position)
            self.__logger.debug("handle_put_fuuid Conserver part %s" % path_fichier)

            if content_hash:
                verificateur = VerificateurHachage(content_hash)
            else:
                verificateur = None
            with open(path_fichier_work, 'wb', buffering=1024 * 1024) as fichier:
                async for chunk in request.content.iter_chunked(64 * 1024):
                    if verificateur:
                        verificateur.update(chunk)
                    fichier.write(chunk)

            # Verifier hachage de la partie
            if verificateur:
                try:
                    verificateur.verify()
                except ErreurHachage as e:
                    self.__logger.info("handle_put_fuuid Erreur verification hachage : %s" % str(e))
                    # Effacer le repertoire pour permettre un re-upload
                    shutil.rmtree(path_upload)
                    return web.HTTPBadRequest()

            # Verifier que la taille sur disque correspond a la taille attendue
            # Meme si le hachage est OK, s'assurer d'avoir conserve tous les bytes
            stat = path_fichier_work.stat()
            if content_length is not None and stat.st_size != content_length:
                self.__logger.info("handle_put_fuuid Erreur verification taille, sauvegarde %d, attendu %d" % (
                stat.st_size, content_length))
                path_fichier_work.unlink(missing_ok=True)
                return web.HTTPBadRequest()

            # Retirer le .work du fichier
            path_fichier_work.rename(path_fichier)

        self.__logger.debug("handle_put_fuuid fuuid: %s position: %s recu OK" % (fuuid, position))
        return web.HTTPOk()

    async def handle_post_fuuid(self, request: Request):
        async with self._semaphore_threads:
            fuuid = request.match_info['fuuid']
            self.__logger.debug("handle_post_fuuid %s" % fuuid)

            headers = request.headers
            if request.body_exists:
                body = await request.json()
                self.__logger.debug("handle_post_fuuid body\n%s" % json.dumps(body, indent=2))
            else:
                # Aucun body - transferer le contenu du fichier sans transactions (e.g. image small)
                body = None

            # Afficher info (debug)
            self.__logger.debug("handle_post_fuuid fuuid: %s" % fuuid)
            for key, value in headers.items():
                self.__logger.debug('handle_post_fuuid key: %s, value: %s' % (key, value))

            # Lire JWT pour recuperer le idmg (sub). C'est aussi une revalidation.
            try:
                jwt_contenu = await parse_jwt(self.__etat, request.headers['X-jwt'])
                idmg = jwt_contenu['sub']
            except:
                self.__logger.exception("Erreur verification JWT")
                return web.HTTPForbidden()

            path_upload = self.get_path_upload_fuuid(idmg, fuuid)

            # Creer commande d'hebergement de fichier
            contenu_commande = {'idmg': idmg, 'fuuid': fuuid}
            formatteur_message = self.__etat.formatteur_message
            transaction, message_id = formatteur_message.signer_message(
                Constantes.KIND_COMMANDE, contenu_commande, 'Hebergement', action='ajouterFichier')
            path_transaction = pathlib.Path(path_upload, ConstantesHebergement.FICHIER_TRANSACTION)
            with open(path_transaction, 'wt') as fichier:
                json.dump(transaction, fichier)

            cles = None

            path_etat = pathlib.Path(path_upload, ConstantesHebergement.FICHIER_ETAT)
            if body is not None:
                # Valider body, conserver json sur disque
                etat = body['etat']
                hachage = etat['hachage']
                with open(path_etat, 'wt') as fichier:
                    json.dump(etat, fichier)

            else:
                # Sauvegarder etat.json sans body
                etat = {'hachage': fuuid, 'retryCount': 0,
                        'created': int(datetime.datetime.utcnow().timestamp() * 1000)}
                hachage = fuuid
                with open(path_etat, 'wt') as fichier:
                    json.dump(etat, fichier)

            # Valider hachage du fichier complet (parties assemblees)
            try:
                job_valider = JobVerifierParts(transaction, path_upload, hachage, cles)
                await self.__queue_verifier_parts.put(job_valider)
                await asyncio.wait_for(job_valider.done.wait(), timeout=20)
                if job_valider.exception is not None:
                    raise job_valider.exception
            except asyncio.TimeoutError:
                self.__logger.info(
                    'handle_post_fuuid Verification fichier %s assemble en cours, repondre HTTP:201' % fuuid)
                return web.HTTPCreated()
            except Exception as e:
                self.__logger.exception(
                    'handle_post_fuuid Erreur verification hachage fichier %s assemble : %s' % (fuuid, e))
                shutil.rmtree(path_upload)
                return web.HTTPFailedDependency()

        return web.HTTPAccepted()

    async def handle_delete_fuuid(self, request: Request):
        async with self._semaphore_threads:
            raise NotImplementedError("todo")

    async def handle_post_backup_verifierfichiers(self, request: Request):
        async with self._semaphore_threads:
            raise NotImplementedError("todo")

    async def handle_put_backup(self, request: Request):
        async with self._semaphore_threads:
            raise NotImplementedError("todo")

    async def handle_get_backup(self, request: Request):
        async with self._semaphore_threads:
            raise NotImplementedError("todo")

    async def thread_verifier_parts(self):
        self.__queue_verifier_parts = asyncio.Queue(maxsize=20)
        pending = [
            asyncio.create_task(self.__stop_event.wait()),
            asyncio.create_task(self.__queue_verifier_parts.get())
        ]
        while self.__stop_event.is_set() is False:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            # Conditions de fin de thread
            if self.__stop_event.is_set() is True:
                for p in pending:
                    p.cancel()
                    try:
                        await p
                    except asyncio.CancelledError:
                        pass  # OK
                    except AttributeError:
                        pass  # Pas une task
                for d in done:
                    if d.exception():
                        raise d.exception()
                return  # Stopped

            for t in done:
                if t.exception():
                    for p in pending:
                        try:
                            p.cancel()
                            await p
                        except asyncio.CancelledError:
                            pass  # OK
                        except AttributeError:
                            pass  # Pas une task

                    raise t.exception()

            for d in done:
                if d.exception():
                    self.__logger.error("thread_verifier_parts Erreur traitement message : %s" % d.exception())
                else:
                    job_verifier_parts: JobVerifierParts = d.result()
                    try:
                        await self.traiter_job_verifier_parts(job_verifier_parts)
                    except Exception as e:
                        self.__logger.exception("thread_verifier_parts Erreur verification hachage %s" % job_verifier_parts.hachage)
                        job_verifier_parts.exception = e
                    finally:
                        # Liberer job
                        job_verifier_parts.done.set()

            if len(pending) == 0:
                raise Exception('arrete indirectement (pending vide)')

            pending.add(asyncio.create_task(self.__queue_verifier_parts.get()))

    async def traiter_job_verifier_parts(self, job: JobVerifierParts):
        try:
            path_upload = job.path_upload
            hachage = job.hachage
            args = [path_upload, hachage]
            # Utiliser thread pool pour validation
            await asyncio.to_thread(valider_hachage_upload_parts, *args)
        except Exception as e:
            self.__logger.exception(
                'traiter_job_verifier_parts Erreur verification hachage fichier %s assemble : %s' % (job.path_upload, e))
            shutil.rmtree(job.path_upload, ignore_errors=True)
            # return web.HTTPFailedDependency()
            raise e

        # Transferer vers intake
        try:
            await self.__intake.ajouter_upload(path_upload)
        except Exception as e:
            self.__logger.exception(
                'handle_post Erreur ajout fichier %s assemble au intake : %s' % (path_upload, e))
            raise e

    async def run(self):
        self.__logger.info("WebConsignation.run Debug")

        pending = [
            asyncio.create_task(self.__stop_event.wait()),
            asyncio.create_task(self.thread_verifier_parts()),
            asyncio.create_task(self.__intake.run(self.__stop_event)),
        ]

        await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        self.__logger.info("WebConsignation.run Fin")

    # async def handle_get_consignation(self, request: Request):
    #     async with self._semaphore_threads:
    #         # Verifier instance-id du token JWT
    #         try:
    #             jwt = request.headers['X-jwt']
    #         except KeyError:
    #             return web.HTTPUnauthorized()
    #
    #         # Transmettre requete au domaine hebergement
    #         try:
    #             producer = await asyncio.wait_for(self.__etat.producer_wait(), 3)
    #         except asyncio.TimeoutError:
    #             self.__logger.error("handle_get_consignation Timeout producer_wait")
    #             return web.HTTPServerError()
    #
    #         requete_hebergement = {
    #         }
    #         try:
    #             reponse = await producer.executer_requete(
    #                 requete_hebergement, 'CoreTopologie', 'getConsignationFichiers', exchange=Constantes.SECURITE_PUBLIC)
    #             reponse = reponse.parsed
    #         except asyncio.TimeoutError:
    #             self.__logger.error("handle_get_jwt Timeout executer_requete")
    #             return web.HTTPServerError()
    #
    #         data = {
    #             'consignation_url': reponse['consignation_url'] + '/hebergement',
    #         }
    #
    #         champs_process = ['url_download', 'sync_intervalle']
    #
    #         for champ in champs_process:
    #             try:
    #                 url_download = reponse[champ]
    #                 if url_download is not None and url_download != '':
    #                     data[champ] = url_download
    #             except KeyError:
    #                 pass
    #
    #         return web.json_response(data)


class Forbidden(Exception):
    pass


async def parse_jwt(etat, jwt):
    jwt_headers = get_headers(jwt)
    kid = jwt_headers['kid']
    enveloppe = await etat.charger_certificat(kid)
    jwt_contenu = verify(enveloppe, jwt)
    return jwt_contenu


def valider_hachage_upload_parts(path_upload: pathlib.Path, hachage: str):
    positions = list()
    for item in path_upload.iterdir():
        if item.is_file():
            nom_fichier = str(item)
            if nom_fichier.endswith('.part'):
                position = int(item.name.split('.')[0])
                positions.append(position)
    positions = sorted(positions)

    verificateur = VerificateurHachage(hachage)

    for position in positions:
        path_fichier = pathlib.Path(path_upload, '%d.part' % position)

        with open(path_fichier, 'rb') as fichier:
            while True:
                chunk = fichier.read(64*1024)
                if not chunk:
                    break
                verificateur.update(chunk)

    verificateur.verify()  # Lance une exception si le hachage est incorrect

