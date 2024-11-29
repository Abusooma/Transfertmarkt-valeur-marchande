import os
import re
import time
import sqlite3
import threading
import unicodedata
import logging
from itertools import permutations, combinations
from itertools import permutations
from rapidfuzz import fuzz
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from selectolax.parser import HTMLParser
from selenium import webdriver
from selenium.webdriver.common.by import By
from loguru import logger
from queue import Queue
from typing import List, Dict, Optional
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
    def __init__(self, max_threads: int = 3):
        self.max_threads = max_threads
        self.cache = CacheSQLite()
        self.pool_drivers = Queue()
        self._initialiser_pool_drivers()


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
                    logger.debug("Popup fermé avec succès")
                driver.switch_to.default_content()
        except Exception as e:
            # logger.debug(f"Erreur lors du traitement du popup: {e}")
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
        nom_joueur = ''.join(
            c for c in unicodedata.normalize('NFD', nom_joueur) if unicodedata.category(c) != 'Mn'
        )
        nom_joueur_nettoyer = re.sub(r"[^a-zA-Z0-9\s\-]", "", nom_joueur).lower().strip()
        return nom_joueur_nettoyer.replace("-", " ")

    
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

    def _scraper_valeur_joueur(self, nom_joueur: str) -> Optional[ValeurJoueur]:
        driver = self.pool_drivers.get()
        try:
            nom_normalise = self._normaliser_nom(nom_joueur)

            variantes_recherche = self._generer_variantes_recherche(nom_normalise)

            meilleur_resultat = None
            meilleur_score = 0
            urls_visitees = set()

            cache_resultats_normals = {}

            for variante in variantes_recherche:
                url_recherche = f"https://www.transfermarkt.fr/schnellsuche/ergebnis/schnellsuche?query={quote(variante)}"

                if url_recherche in urls_visitees:
                    continue
                urls_visitees.add(url_recherche)

                try:
                    driver.get(url_recherche)
                    table = self._obtenir_table(driver)

                    if not table:
                        continue

                    lignes = table.css("tr")
                    for i, ligne in enumerate(lignes[1:], 1):  # Skip header row
                        try:
                            element_nom = ligne.css_first("td.hauptlink a[title]")
                            if not element_nom:
                                continue

                            nom_transfermarkt = element_nom.attributes.get(
                                'title', '')
                            nom_normalise_transfermarkt = self._normaliser_nom(
                                nom_transfermarkt)

                            if nom_normalise_transfermarkt in cache_resultats_normals:
                                resultat = cache_resultats_normals[nom_normalise_transfermarkt]
                            else:
                                statut_element = ligne.css_first("td")
                                valeur_element = ligne.css_first(
                                    "td.rechts.hauptlink")

                                if statut_element and "Fin de carrière" in statut_element.text(strip=True):
                                    statut = "Fin de carrière"
                                    valeur = -1
                                else:
                                    statut = "actif"
                                    valeur = 0.0
                                    if valeur_element:
                                        valeur_texte = valeur_element.text(
                                            strip=True)
                                        if valeur_texte:
                                            valeur = self._parser_valeur_marche(
                                                valeur_texte)

                                resultat = {
                                    'nom': nom_transfermarkt,
                                    'valeur': valeur,
                                    'statut': statut
                                }

                                cache_resultats_normals[nom_normalise_transfermarkt] = resultat

                            score = fuzz.token_sort_ratio(nom_normalise, nom_normalise_transfermarkt)

                            if score >= 60 and (score > meilleur_score or
                                                (score == meilleur_score and resultat['valeur'] >
                                                (meilleur_resultat['valeur'] if meilleur_resultat else -float('inf')))):
                                meilleur_score = score
                                meilleur_resultat = {
                                    **resultat,
                                    'score': score
                                }

                        except Exception as e:
                            logger.debug(
                                f"Erreur lors du traitement d'un joueur dans la ligne: {e}")
                            continue

                except Exception as e:
                    logger.debug(
                        f"Erreur lors du traitement de la variante {variante}: {e}")
                    continue

            if meilleur_resultat:
                return ValeurJoueur(
                    nom_joueur,
                    meilleur_resultat['nom'],
                    meilleur_resultat['valeur'],
                    meilleur_resultat['statut']
                )

            return ValeurJoueur(
                nom_joueur,
                "",
                0.0,
                "inconnu",
                "Aucun joueur actif trouvé avec ce nom"
            )

        except Exception as e:
            logger.error(f"Erreur lors du scraping de {nom_joueur}: {e}")
            return ValeurJoueur(nom_joueur, "", 0.0, "inconnu", str(e))

        finally:
            self.pool_drivers.put(driver)
            
    def recuperer_valeurs_joueurs(self, noms_joueurs: List[str]) -> Dict[str, ValeurJoueur]:
        resultats = {}
        total_joueurs = len(noms_joueurs)
        joueurs_traites = 0
        mises_a_jour_reussies = 0

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(
                self._scraper_valeur_joueur, nom): nom for nom in noms_joueurs}
            for future in as_completed(futures):
                valeur = future.result()
                resultats[valeur.nom_original] = valeur

                joueurs_traites += 1

                if valeur.valeur > 0 or valeur.statut != "inconnu":
                    self.cache.definir(valeur.nom_original, valeur)
                    mises_a_jour_reussies += 1

                print(f"\nProgression - Joueurs traités : {joueurs_traites}/{total_joueurs}, "
                      f"Mises à jour réussies : {mises_a_jour_reussies}")

        return resultats
    
    def fermer(self):
        while not self.pool_drivers.empty():
            driver = self.pool_drivers.get()
            driver.quit()

