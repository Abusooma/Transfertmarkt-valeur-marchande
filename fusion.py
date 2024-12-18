import pandas as pd
from pathlib import Path
from datetime import datetime
from openpyxl import load_workbook

mois_fr = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril",
    5: "mai", 6: "juin", 7: "juillet", 8: "août",
    9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
}
mois_fr_inverse = {v: k for k, v in mois_fr.items()}


def convertir_date(date_obj):
    """
    Convertit une date au format standard.
    """
    if isinstance(date_obj, (datetime, pd.Timestamp)):
        return date_obj

    if pd.isna(date_obj):
        return None

    date_obj = str(date_obj).strip()

    try:
        try:
            jour, mois, annee = date_obj.split(' ')
            mois_num = mois_fr_inverse[mois.lower()]
            date_str = f"{jour} {mois_num} {annee}"
            return datetime.strptime(date_str, '%d %m %Y')
        except (ValueError, KeyError):
            pass

        formats = [
            '%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d',
            '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d',
            '%d %m %Y', '%d.%m.%Y'
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_obj, fmt)
            except (ValueError, AttributeError):
                continue

        raise ValueError(f"Impossible de convertir la date : {date_obj}")

    except Exception as e:
        print(str(e))
        return None

def formater_date_fr(date_obj):
    """
    Formate une date (datetime) au format '25 août 1996'.
    """
    if pd.isna(date_obj):
        return None

    jour = date_obj.day
    mois = mois_fr[date_obj.month]
    annee = date_obj.year
    return f"{jour} {mois} {annee}"


def fusionner_fichiers_excel(dossier_entree, fichier_sortie):
    """
    Fusionne tous les fichiers Excel dans un dossier en un seul fichier.
    
    :param dossier_entree: Chemin du dossier contenant les fichiers Excel
    :param fichier_sortie: Chemin du fichier Excel de sortie
    """
    dataframes = []

    total_fichiers_traites = 0
    total_fichiers_echoues = 0

    for fichier in Path(dossier_entree).rglob('*'):
        if fichier.suffix.lower() in ['.xls', '.xlsx']:
            try:
                df = pd.read_excel(
                    fichier,
                    usecols=['NOM', 'DOB', 'DATE', 'VALEUR', 'NOM2'],
                    dtype={'NOM': str, 'DOB': str, 'DATE': str,
                           'VALEUR': str, 'NOM2': str}
                )

                df['DOB'] = df['DOB'].fillna('').apply(convertir_date)

                df['DOB'] = df[df['DOB'].notna()]['DOB'].apply(formater_date_fr)

                dataframes.append(df)

                total_fichiers_traites += 1
                print(f"Fichier traité avec succès : {fichier.name}")

            except Exception as e:
                print(
                    f"Erreur lors du traitement du fichier {fichier.name}: {e}")
                total_fichiers_echoues += 1

    if not dataframes:
        print("Aucun fichier Excel n'a été trouvé.")
        return

    df_final = pd.concat(dataframes, ignore_index=True)

    df_final.drop_duplicates(inplace=True)

    df_final.to_excel(fichier_sortie, index=False, engine='openpyxl')

    workbook = load_workbook(fichier_sortie)
    worksheet = workbook.active

    for col in worksheet.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except Exception:
                pass
        adjusted_width = min(max_length + 2, 50)  # Limiter la largeur maximale
        worksheet.column_dimensions[col_letter].width = adjusted_width

    workbook.save(fichier_sortie)

    print(f"\nRapport de fusion des fichiers Excel :")
    print(f"- Fichiers traités avec succès : {total_fichiers_traites}")
    print(f"- Fichiers en échec : {total_fichiers_echoues}")
    print(f"Fichier final créé : {fichier_sortie.name}")
    print(f"Nombre total de lignes : {len(df_final)}")


if __name__ == "__main__":
    base_dir = Path(__file__).parent
    dossier_source = base_dir / "resultats"
    fichier_de_sortie = base_dir / "fichier_final.xlsx" # remplace par le nom souhaité

    fusionner_fichiers_excel(dossier_source, fichier_de_sortie)
