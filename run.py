from loguru import logger
import asyncio
from datetime import datetime
import time
import sys
import threading
import pandas as pd
from openpyxl.utils import get_column_letter
from openpyxl import load_workbook
from players import ScraperTransferMarkt


logger.remove()
logger.add(sys.stdout, level="INFO", colorize=True,
           format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")
logger.add("logs/fichier.log", level="ERROR",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", rotation="1 MB")


class RealTimeChronometre:
    def __init__(self):
        self.debut = time.time()
        self.arret = False
        self.thread = None

    def demarrer(self):
        def afficher_temps():
            while not self.arret:
                temps_ecoule = time.time() - self.debut
                minutes = int(temps_ecoule // 60)
                secondes = int(temps_ecoule % 60)
                sys.stdout.write(
                    f"\rTemps écoulé: {minutes:02d}:{secondes:02d}")
                sys.stdout.flush()
                time.sleep(1)

        self.thread = threading.Thread(target=afficher_temps, daemon=True)
        self.thread.start()

    def arreter(self):
        self.arret = True
        if self.thread:
            self.thread.join()
        temps_total = time.time() - self.debut
        return temps_total

class MiseAJourValeursJoueurs:
    def __init__(self, fichier_entree: str, fichier_sortie: str):
        self.fichier_entree = fichier_entree
        self.fichier_sortie = fichier_sortie
        self.scraper = ScraperTransferMarkt(max_threads=3)
        self.chronometre = RealTimeChronometre()

    def convertir_date(self, date_str):
        formats_possibles = ["%d/%m/%Y",
                             "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"]
        for fmt in formats_possibles:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue
    
        return pd.NaT

    def formater_nom2(self, nom_original: str) -> str:
        parties = nom_original.split()

        index_majuscule = next(
            (i for i, mot in enumerate(parties) if mot.isupper()), None)

        if index_majuscule is not None:
            mot_majuscule = parties[index_majuscule]
            autres_parties = parties[:index_majuscule] + \
                parties[index_majuscule+1:]

            return f"{mot_majuscule} {' '.join(autres_parties)}"

        return nom_original


    async def mettre_a_jour(self):
        logger.info("Début du Processus")
        self.chronometre.demarrer()

        df = pd.read_excel(self.fichier_entree, dtype={"NOM": str})
        noms_joueurs = df["NOM"].tolist()

        try:
            valeurs_joueurs = await asyncio.to_thread(
                self.scraper.recuperer_valeurs_joueurs,
                noms_joueurs
            )
        except Exception as e:
            logger.error(f"Erreur durant le scraping : {e}")
            self.chronometre.arreter()
            raise

        donnees_mises_a_jour = []
        date_courante = datetime.now().strftime("%d/%m/%Y")

        for _, ligne in df.iterrows():
            nom = ligne["NOM"]
            valeur_joueur = valeurs_joueurs.get(nom, None)

            if valeur_joueur and valeur_joueur.nom_transfermarkt:
                parties_nom = valeur_joueur.nom_transfermarkt.split()
                if len(parties_nom) >= 3:
                    nom2 = f"{parties_nom[-2].upper()} {parties_nom[-1].upper()} {' '.join(parties_nom[:-2])}"
                else:
                    nom2 = f"{parties_nom[-1].upper()} {' '.join(parties_nom[:-1])}"
            else:
                nom2 = self.formater_nom2(nom)

            donnees_mises_a_jour.append({
                "NOM": valeur_joueur.nom_transfermarkt if valeur_joueur else "",
                "DOB": valeur_joueur.date_naissance if valeur_joueur and valeur_joueur.date_naissance else "",
                "DATE": date_courante,
                "VALEUR": valeur_joueur.valeur if valeur_joueur else 0.0,
                "NOM2": nom2,
                "FIN-CONTRAT": valeur_joueur.fin_contrat if valeur_joueur and valeur_joueur.fin_contrat else ""
            })

        df_mise_a_jour = pd.DataFrame(donnees_mises_a_jour)
        df_mise_a_jour.to_excel(self.fichier_sortie, index=False)

        self.ajuster_largeur_colonnes(self.fichier_sortie)

        temps_total = self.chronometre.arreter()

        logger.info(
            f"Processus terminé. Fichier enregistré sous {self.fichier_sortie}")
        logger.info(f"Temps total d'exécution : {temps_total:.2f} secondes")


    def ajuster_largeur_colonnes(self, fichier: str):
        wb = load_workbook(fichier)
        sheet = wb.active

        for col in sheet.columns:
            max_length = 0
            col_letter = get_column_letter(col[0].column)

            for cell in col:
                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except:
                    pass

            adjusted_width = max_length + 2
            sheet.column_dimensions[col_letter].width = adjusted_width

        wb.save(fichier)

async def main():
    fichier_entree = "pour-inverser-sortie1.xls"
    fichier_sortie = "resultat_essai.xlsx"

    mise_a_jour = MiseAJourValeursJoueurs(fichier_entree, fichier_sortie)
    try:
        await mise_a_jour.mettre_a_jour()
    except Exception as e:
        logger.error(f"Une erreur s'est produite : {e}")
    finally:
        mise_a_jour.scraper.cache.fermer()

if __name__ == "__main__":
    asyncio.run(main())
