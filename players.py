import os
import re
import time
import sqlite3
import threading
import unicodedata
import logging
from itertools import permutations, combinations
from rapidfuzz import fuzz
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from selectolax.parser import HTMLParser
from selenium import webdriver
from selenium.webdriver.common.by import By
from loguru import logger
from queue import Queue
from typing import Any, List, Dict, Optional
from dataclasses import dataclass


logging.getLogger('tensorflow').setLevel(logging.ERROR)
logging.getLogger('absl').setLevel(logging.ERROR)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'


@dataclass
class ValeurJoueur:
    nom_original: str
    nom_transfermarkt: str
    valeur: float
    statut: str = "actif"
    fin_contrat: str = None
    date_naissance: str = None
    controle: str = ""
    erreur: Optional[str] = None
    timestamp: float = time.time()


class CacheSQLite:
    def __init__(self, db_path="cache.db", duree_cache=3600):
        self._db_path = db_path
        self.duree_cache = duree_cache
        self._thread_local = threading.local()
        self._create_table()

    def _get_connection(self):
        if not hasattr(self._thread_local, 'connection'):
            self._thread_local.connection = sqlite3.connect(self._db_path)
        return self._thread_local.connection

    def _create_table(self):
        conn = self._get_connection()
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    nom_joueur TEXT PRIMARY KEY,
                    nom_transfermarkt TEXT,
                    valeur REAL,
                    statut TEXT,
                    erreur TEXT,
                    fin_contrat TEXT,
                    date_naissance TEXT,
                    timestamp INTEGER
                )
            """)

    def obtenir(self, nom_joueur: str) -> Optional[ValeurJoueur]:
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT * FROM cache WHERE nom_joueur = ?", (nom_joueur,))
        row = cursor.fetchone()
        if row and time.time() - row[5] <= self.duree_cache:
            return ValeurJoueur(*row)
        return None

    def definir(self, nom_joueur: str, valeur: ValeurJoueur):
        conn = self._get_connection()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (nom_joueur, nom_transfermarkt, valeur, statut, erreur, timestamp) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (nom_joueur, valeur.nom_transfermarkt, valeur.valeur,
                 valeur.statut, valeur.erreur, valeur.timestamp)
            )

    def fermer(self):
        if hasattr(self._thread_local, 'connection'):
            self._thread_local.connection.close()
            del self._thread_local.connection


class ScraperTransferMarkt:
    BASE_URL = "https://www.transfermarkt.fr"

    def __init__(self, max_threads: int = 3):
        self.max_threads = max_threads
        self.cache = CacheSQLite()
        self.pool_drivers = Queue()
        self._initialiser_pool_drivers()
        self.joueurs_non_traites = []

    def _initialiser_pool_drivers(self):
        for _ in range(self.max_threads):
            driver = self._creer_driver()
            self.pool_drivers.put(driver)

    def _creer_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--log-level=3")

        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)

        driver = webdriver.Chrome(options=options)
        driver.set_window_size(1920, 1080)
        driver.implicitly_wait(5)
        driver.set_page_load_timeout(30)
        return driver

    def _traiter_popup(self, driver):
        try:
            iframe = driver.find_elements(By.ID, "sp_message_iframe_953822")
            if iframe:
                driver.switch_to.frame(iframe[0])
                html = HTMLParser(driver.page_source)
                bouton = html.css_first(
                    'button.message-component.message-button.no-children.focusable.accept-all.sp_choice_type_11')
                if bouton:
                    bouton.click()
                driver.switch_to.default_content()
        except Exception as e:
            driver.switch_to.default_content()

    def _obtenir_table(self, driver) -> Optional[HTMLParser]:
        for _ in range(2):
            html = HTMLParser(driver.page_source)
            table = html.css_first("table.items")
            if table:
                return table
            self._traiter_popup(driver)
        return None

    def _normaliser_nom(self, nom_joueur: str) -> str:
        try:
            nom_joueur = nom_joueur.replace('æ', 'ae').replace('Æ', 'AE')

            nom_joueur = ''.join(
                c for c in unicodedata.normalize('NFD', nom_joueur) if unicodedata.category(c) != 'Mn'
            )

            nom_joueur_nettoyer = re.sub(
                r"[^a-zA-Z0-9\s\-]", "", nom_joueur).lower().strip()

            return nom_joueur_nettoyer.replace("-", " ")
        except Exception as e:
            logger.error(
                f"Erreur lors de la normalisation du nom de joueur: {e}")
            return nom_joueur

    def _generer_variantes_recherche(self, nom_joueur: str) -> list:
        noms = [nom for nom in nom_joueur.split() if nom]
        variantes = []

        variantes.append(nom_joueur)

        for taille in range(1, len(noms) + 1):
            combinaisons = list(combinations(noms, taille))

            for combo in combinaisons:
                perms = list(permutations(combo))

                for perm in perms:
                    variante = " ".join(perm)
                    variantes.append(variante)

        variantes_sans_accents = []
        for variante in variantes:
            variante_normalisee = self._normaliser_nom(variante)
            if variante_normalisee != variante:
                variantes_sans_accents.append(variante_normalisee)

        variantes.extend(variantes_sans_accents)

        return list(dict.fromkeys(variantes))

    def _parser_valeur_marche(self, valeur_texte: str) -> float:
        try:
            match = re.search(r"(\d+(?:,\d+)?)\s*(mio\.|K)", valeur_texte)
            if not match:
                return 0.0
            valeur, unite = match.groups()
            valeur = float(valeur.replace(",", "."))
            return valeur if unite == "mio." else valeur / 1000
        except Exception:
            return 0.0

    def _parser_valeur_fin_contrat(self, html):
        try:
            contrat_spans = html.css('span')

            for i, span in enumerate(contrat_spans):
                text = span.text(strip=True)
                if "Contrat jusqu'à:" in text:
                    if i + 1 < len(contrat_spans):
                        next_span = contrat_spans[i + 1]
                        value_end_date = next_span.text(strip=True)

                        if value_end_date == '-' or not value_end_date:
                            return '?'
                        return value_end_date

            logger.warning("Aucune date de fin de contrat trouvée")
            return '?'

        except Exception as e:
            logger.error(f"Erreur lors du parsing de la fin de contrat: {e}")
            return None

    def _parser_date_naissance(self, html):
        try:
            items = html.css('div.data-header__details ul.data-header__items li')
            for item in items:
                if 'Naissance' in item.text():
                    span = item.css_first('span[itemprop="birthDate"]')
                    if span:
                        date_naissance = span.text(strip=True)
                        date_naissance = date_naissance.split('(')[0].strip()
                        return date_naissance
            logger.warning("Aucune date de naissance trouvée")
            return None
        except Exception as e:
            logger.error(f"Erreur lors du parsing de la date de naissance: {e}")
            return None
                

    def _recuperer_fin_contrat(self, driver, url_details):
        try:
            driver.get(url_details)

            WebDriverWait(driver, 5)
            self._traiter_popup(driver)

            html = HTMLParser(driver.page_source)

            return self._parser_valeur_fin_contrat(html)

        except Exception as e:
            logger.warning(
                f"Erreur lors de la récupération de la fin de contrat: {e}")
            return None
         
    def _creer_valeur_joueur_court(self, nom_joueur: str) -> ValeurJoueur:
        """Crée un ValeurJoueur pour les noms courts nécessitant une vérification."""
        return ValeurJoueur(
            nom_joueur,
            nom_joueur,
            0.0,
            "actif",
            "",
            "",
            "A verifier",
            None,
            time.time()
        )


    def _creer_valeur_joueur_erreur(self, nom_joueur: str, erreur: str) -> ValeurJoueur:
        """Crée un ValeurJoueur pour les cas d'erreur."""
        return ValeurJoueur(
            nom_original=nom_joueur,
            nom_transfermarkt=nom_joueur,
            valeur=0.0,
            statut="actif",
            fin_contrat="",
            date_naissance="",
            controle="A verifier",
            erreur=erreur,
            timestamp=time.time()
        )


    def _analyser_ligne_resultat(self, ligne, nom_normalise: str, cache_resultats_normals: dict):
        """Analyse une ligne de résultat et retourne les informations extraites."""
        element_nom = ligne.css_first("td.hauptlink a[title]")
        if not element_nom:
            return None

        nom_transfermarkt = element_nom.attributes.get('title', '')
        if not nom_transfermarkt:
            return None

        nom_normalise_transfermarkt = self._normaliser_nom(nom_transfermarkt)
        url_details = urljoin(
            self.BASE_URL, element_nom.attributes.get('href', ''))

        # Calcul des scores de similarité
        score_token = fuzz.token_sort_ratio(
            nom_normalise, nom_normalise_transfermarkt)
        score_partial = fuzz.partial_ratio(
            nom_normalise, nom_normalise_transfermarkt)
        score_set = fuzz.token_set_ratio(
            nom_normalise, nom_normalise_transfermarkt)
        score = max(score_token, score_partial, score_set)

        if nom_normalise_transfermarkt in cache_resultats_normals:
            resultat = cache_resultats_normals[nom_normalise_transfermarkt]
        else:
            resultat = self._extraire_info_joueur(ligne)
            cache_resultats_normals[nom_normalise_transfermarkt] = resultat

        return {
            'score': score,
            'url_details': url_details,
            'resultat': resultat
        }


    def _extraire_info_joueur(self, ligne) -> dict:
        """Extrait les informations d'un joueur à partir d'une ligne."""
        statut_element = ligne.css_first("td")
        valeur_element = ligne.css_first("td.rechts.hauptlink")

        if statut_element and "Fin de carrière" in statut_element.text(strip=True):
            statut = "Fin de carrière"
            valeur = -1
        else:
            statut = "actif"
            valeur = 0.0
            if valeur_element:
                valeur_texte = valeur_element.text(strip=True)
                if valeur_texte:
                    valeur = self._parser_valeur_marche(valeur_texte)

        return {
            'nom': ligne.css_first("td.hauptlink a[title]").attributes.get('title', ''),
            'valeur': valeur,
            'statut': statut
        }


    def _rechercher_meilleur_resultat(self, driver, variantes_recherche: list, nom_normalise: str):
        """Recherche le meilleur résultat parmi toutes les variantes."""
        meilleur_resultat = None
        meilleur_url_details = None
        meilleur_score = 0
        urls_visitees = set()
        cache_resultats_normals = {}

        for variante in variantes_recherche:
            url_recherche = f"{self.BASE_URL}/schnellsuche/ergebnis/schnellsuche?query={quote(variante)}"

            if url_recherche in urls_visitees:
                continue

            urls_visitees.add(url_recherche)

            try:
                driver.get(url_recherche)
                table = self._obtenir_table(driver)

                if not table:
                    logger.debug(
                        f"Pas de résultats pour la variante: '{variante}'")
                    continue

                lignes = table.css("tr")

                for ligne in lignes[1:]:
                    try:
                        resultat_analyse = self._analyser_ligne_resultat(
                            ligne, nom_normalise, cache_resultats_normals)

                        if not resultat_analyse:
                            continue

                        if (resultat_analyse['score'] >= 90 and
                            (resultat_analyse['score'] > meilleur_score or
                            (resultat_analyse['score'] == meilleur_score and
                            resultat_analyse['resultat']['valeur'] >
                            (meilleur_resultat['valeur'] if meilleur_resultat else -float('inf'))))):

                            meilleur_score = resultat_analyse['score']
                            meilleur_resultat = {
                                **resultat_analyse['resultat'],
                                'score': resultat_analyse['score']
                            }
                            meilleur_url_details = resultat_analyse['url_details']

                    except Exception as e:
                        logger.error(
                            f"Erreur lors de l'analyse d'une ligne: {str(e)}")
                        continue

            except Exception as e:
                logger.error(
                    f"Erreur lors du traitement de la variante {variante}: {str(e)}")
                continue

        return meilleur_resultat, meilleur_url_details


    def _finaliser_valeur_joueur(self, driver, meilleur_resultat: dict, meilleur_url_details: str, nom_joueur: str) -> ValeurJoueur:
        """Finalise la création du ValeurJoueur avec les informations détaillées."""
        try:
            driver.get(meilleur_url_details)
            WebDriverWait(driver, 5)
            self._traiter_popup(driver)
            html = HTMLParser(driver.page_source)

            date_naissance = self._parser_date_naissance(html)

            if meilleur_resultat['valeur'] == -1 or meilleur_resultat['statut'] == "Fin de carrière":
                fin_contrat = "fin de carriere"
            else:
                fin_contrat = self._parser_valeur_fin_contrat(html)

            return ValeurJoueur(
                nom_joueur,
                meilleur_resultat['nom'],
                meilleur_resultat['valeur'],
                meilleur_resultat['statut'],
                fin_contrat,
                date_naissance,
                None,
                None,
                time.time()
            )
        except Exception as e:
            logger.error(
                f"Erreur lors de la finalisation de ValeurJoueur: {str(e)}")
            raise


    def _scraper_valeur_joueur(self, nom_joueur: str) -> Optional[ValeurJoueur]:
        """Méthode principale de scraping des valeurs des joueurs."""
        driver = self.pool_drivers.get()
        try:
            if len(nom_joueur.strip()) < 7:
                return self._creer_valeur_joueur_court(nom_joueur)

            nom_normalise = self._normaliser_nom(nom_joueur)
            variantes_recherche = self._generer_variantes_recherche(nom_normalise)

            meilleur_resultat, meilleur_url_details = self._rechercher_meilleur_resultat(
                driver, variantes_recherche, nom_normalise)

            if meilleur_resultat:
                return self._finaliser_valeur_joueur(
                    driver, meilleur_resultat, meilleur_url_details, nom_joueur)

            logger.warning(f"Aucun résultat trouvé pour '{nom_joueur}'")
            return self._creer_valeur_joueur_erreur(
                nom_joueur, f"Aucun joueur trouvé avec le nom {nom_joueur}")

        except Exception as e:
            logger.error(
                f"Erreur globale lors du scraping de {nom_joueur}: {str(e)}")
            return self._creer_valeur_joueur_erreur(nom_joueur, str(e))

        finally:
            self.pool_drivers.put(driver)


    def recuperer_valeurs_joueurs(self, noms_joueurs: List[str]) -> Dict[str, ValeurJoueur]:
        self.joueurs_non_traites = []

        resultats = {}
        total_joueurs = len(noms_joueurs)
        joueurs_traites = 0
        mises_a_jour_reussies = 0

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(
                self._scraper_valeur_joueur, nom): nom for nom in noms_joueurs}
            for future in as_completed(futures):
                try:
                    valeur = future.result()
                    resultats[valeur.nom_original] = valeur

                    joueurs_traites += 1

                    if valeur.valeur > 0 or valeur.statut != "inconnu":
                        self.cache.definir(valeur.nom_original, valeur)
                        mises_a_jour_reussies += 1
                    else:
                        self.joueurs_non_traites.append({
                            'nom': valeur.nom_original,
                            'erreur': valeur.erreur or 'Traitement incomplet'
                        })

                        logger.warning(
                            f"Joueur non traité: {valeur.nom_original} - {valeur.erreur}")

                except Exception as e:

                    logger.error(f"Erreur inattendue pour un joueur: {e}")
                    self.joueurs_non_traites.append({
                        'nom': futures[future],
                        'erreur': str(e)
                    })

                print(f"\nProgression - Joueurs traités : {joueurs_traites}/{total_joueurs}, "
                      f"Mises à jour réussies : {mises_a_jour_reussies}, "
                      f"Joueur en cours : {valeur.nom_original}")

        if self.joueurs_non_traites:
            print("\n--- Joueurs non traités ---")
            for joueur in self.joueurs_non_traites:
                print(f"Nom: {joueur['nom']}, Erreur: {joueur['erreur']}")
            print(
                f"Total joueurs non traités : {len(self.joueurs_non_traites)}")

        return resultats


    def fermer(self):
        while not self.pool_drivers.empty():
            driver = self.pool_drivers.get()
            driver.quit()
